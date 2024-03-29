"""
Prepare predictions from revision score events for elasticsearch ingestion

Accepts two different ways of reading predictions:
* Table of events conforming to the mediawiki/revision/score jsonschema from
mediawiki/event-schemas repository. Table is expected to be partitioned hourly
with year/month/day/hour partition keys.
* Table of rows matching ores_bulk_ingest.py output, for bulk updating of scores.

Ingested predictions are filtered using per-wiki/topic thresholds to the set of
predictions that are thought to accurately represent the page and then format
the predictions into strings for elasticsearch ingestion.  Output table is
structured to be passed on to convert_to_esbulk.py.
"""
from argparse import ArgumentParser
from datetime import datetime
import json
import logging
import sys
from typing import cast, Callable, Mapping, Optional, Sequence, Set

from pyspark.sql import (
    SparkSession, Column, DataFrame, Row, Window,
    functions as F, types as T)
from wmf_spark import HivePartition, HivePartitionTimeRange, HivePartitionWriter


# Probabilities need to be expressed as an integer to integrate into the
# search engine. Scale probabilities by this value before casting to int.
# This must match the max_tf value configured for the term_freq analyzer
# in the elasticsearch field data is indexed into.
SCALE_FACTOR = 1000

# If no threshold is available the prediction must have at least
# this value to be emitted
DEFAULT_THRESHOLD = 0.9


def make_stringify_prediction(
    thresholds: Mapping[str, Mapping[str, float]]
) -> Callable[[str, Mapping[str, float]], Sequence[str]]:
    """Make function to format predictions for elasticsearch ingestion.

    To index the probabilities into the search engine we convert the
    probabilities into an integer score and format as `<token>|<score>`. The
    token is the exact searchable value, and the score is interpreted as the
    token term frequency.

    The set of topics emitted is conditioned on a mapping from the topic
    to a predefined threshold. The prediction must meet the minimum
    per-wiki/topic threshold or it will not be emitted.
    """
    def fn(wiki: str, probabilities: Mapping[str, float]) -> Sequence[str]:
        if any('|' in topic for topic in probabilities.keys()):
            raise Exception('Topic names must not contain |')
        empty = cast(Mapping[str, float], {})
        return [
            '{}|{}'.format(topic, int(SCALE_FACTOR * prob))
            for topic, prob in probabilities.items()
            if prob >= thresholds.get(wiki, empty).get(topic, DEFAULT_THRESHOLD)
        ]

    return fn


def top_row_per_group(
    df: DataFrame,
    partition_by: Sequence[Column],
    order_by: Sequence[Column]
) -> DataFrame:
    """Filter to only the first ordered row per group"""
    w = Window.partitionBy(*partition_by).orderBy(*order_by)
    return (
        df
        .withColumn('_rn', F.row_number().over(w))
        # row_number is 1-indexed. Go figure.
        .where(F.col('_rn') == 1)
        .drop('_rn')
    )


def propagate_by_wbitem(
    df_predictions: DataFrame,
    df_wikibase: DataFrame,
    prediction_col: str,
    # Set of wikis that predictions come from. Scores will not propagate to them
    source_wikis: Set[str],
    # Wiki to propagate prediction from. If no prediction is available from this
    # wiki then no scores are propagated.
    preferred_wiki: str
) -> DataFrame:
    """Propagate predictions from source wiki to all wikis

    The basic algorithm here is to collect together the set of all predictions
    for a single wikibase item, along with the set of all pages for a single
    wikibase item, and then emit predictions about all of the pages based on
    the predictions about the item itself. There should never be more than
    perhaps 1k items on either side, so the row size should be capped at a
    reasonable level.
    """
    def resolve_propagation(
        predictions: Sequence[Row],
        pages: Optional[Sequence[Row]]
    ):
        # All predictions must be emitted as-is.
        out = [(p.wikiid, p.page_id, p.page_namespace, p[prediction_col]) for p in predictions]

        # If pages is None then this wikid/page_id pair was not found
        # in the dataset used for propagation. Emit only the source rows.
        if pages is None or not pages:
            return out

        # Make a quick sanity check that we expected predictions from these wikis
        assert all(p.wikiid in source_wikis for p in predictions)

        # Pages that are linked and not on a wiki the predictions will have
        # the preferred prediction selected for them.
        for p in predictions:
            if p.wikiid == preferred_wiki:
                preferred = p[prediction_col]
                break
        else:
            # No preferred prediction exists
            preferred = None

        if preferred is not None:
            for page in pages:
                if page.wikiid in source_wikis:
                    # We do not propagate scores to wikis that generate their own predictions,
                    # those are only provided by the model directly.
                    continue
                out.append((page.wikiid, page.page_id, page.page_namespace, preferred))

        return out

    # Ensure we have the set of columns we think we do
    df_wikibase = df_wikibase.select('wikiid', 'page_id', 'page_namespace', 'wikibase_item')
    df_predictions = df_predictions.select(
        'wikiid', 'page_id', 'page_namespace', prediction_col)

    df_pages_by_link = (
        df_wikibase
        .groupBy('wikibase_item')
        .agg(F.collect_list(F.struct('wikiid', 'page_id', 'page_namespace')).alias('pages'))
    )

    df_predictions_by_link = (
        df_predictions
        # Both sides of the join have page_namespace and they should be the
        # same.  We could join on the page_namespace, but it's not part of
        # identifying a unique page. In the rare case they vary (perhaps a page
        # move) we shouldn't throw out the prediction, so drop page_namespace
        # from df being joined.
        .join(df_wikibase.drop('page_namespace'), how='left', on=['wikiid', 'page_id'])
        # Not every prediction will have a wikibase_item, but we don't want
        # to pull them into a single giant row. Assign fake wikibase_item strings
        # that are unique per prediction to avoid skew.
        .withColumn('wikibase_item', F.coalesce(
            F.col('wikibase_item'),
            F.concat_ws('-', F.lit('notfound'), F.col('wikiid'), F.col('page_id').cast(T.StringType()))))
        .groupBy('wikibase_item')
        .agg(F.collect_list(F.struct(
            'wikiid', 'page_id', 'page_namespace', prediction_col
        )).alias('predictions'))
    )

    propagate_udf = F.udf(
        resolve_propagation,
        T.ArrayType(T.StructType([
            T.StructField('wikiid', T.StringType()),
            T.StructField('page_id', T.IntegerType()),
            T.StructField('page_namespace', T.IntegerType()),
            T.StructField(prediction_col, T.ArrayType(T.StringType()))
        ])))

    return (
        df_predictions_by_link
        .join(df_pages_by_link, how='left', on=['wikibase_item'])
        .select(F.explode(propagate_udf('predictions', 'pages')).alias('p'))
        .select('p.*')
    )


def extract_prediction(
    df: DataFrame,
    prediction: str,
    thresholds: Mapping[str, Mapping[str, float]],
) -> DataFrame:
    """Extract and format predictions from the shared input schema"""
    # Reshape for shipping to elasticesarch
    stringify_prediction_udf = F.udf(
        make_stringify_prediction(thresholds),
        T.ArrayType(T.StringType()))

    return (
        df.select(
            F.col('wikiid'),
            F.col('page_id'),
            F.col('page_namespace'),
            stringify_prediction_udf(
                F.col('wikiid'),
                F.col('probability')
            ).alias(prediction))
        # Writing out empty arrays fails with parquet output, looks
        # similar to Spark-25271 (resolved in spark 3). For now drop
        # the empty arrays. This has the downside that we have no
        # way to clear previously indexed predictions, we can't send
        # an empty array and have the previous set replaced.
        .where(F.size(F.col(prediction)) > 0)
    )


def load_mediawiki_revision_score(
    df: DataFrame, prediction: str
) -> DataFrame:
    """Load probabilities from mediawiki/revision/score events"""
    # Only take predictions that involve the model we care about
    df_filtered = df.where(F.col('scores')[prediction].isNotNull())

    # A given page may have multiple predictions in the time
    # span. Keep only the most recent prediction per page
    df_transformed = top_row_per_group(
        df_filtered,
        [F.col('database'), F.col('page_id')],
        [F.col('rev_timestamp').desc()])

    # Re-shape to our simplified shared format
    return df_transformed.select(
        F.col('database').alias('wikiid'),
        F.col('page_id'),
        F.col('page_namespace'),
        F.col('scores')[prediction].probability.alias('probability'))


def load_ores_bulk_ingest(
    df: DataFrame, prediction: str
) -> DataFrame:
    """Load probabilities from bulk export

    Note that while the export works on a per-wiki basis we do
    not do any per-wiki filtering. All desired wikis should be
    exported and a single invocation of this script should prepare
    the export for shipping to elasticsearch.
    """
    # Verify we have the expected fields
    return df.select('wikiid', 'page_id', 'page_namespace', 'probability')


INPUT_KINDS = {
    'mediawiki_revision_score': load_mediawiki_revision_score,
    'ores_bulk_ingest': load_ores_bulk_ingest,
}


def arg_parser() -> ArgumentParser:
    def date(val: str) -> datetime:
        return datetime.strptime(val, '%Y-%m-%d')

    def json_path(file_path: str) -> Mapping:
        with open(file_path, 'r') as f:
            return json.load(f)

    parser = ArgumentParser()
    parser.add_argument(
        '--input-partition', required=True, type=HivePartitionTimeRange.from_spec,
        help='Specification for input partition(s). Exact format depends '
             'on the --input-kind specified.')
    parser.add_argument(
        '--input-kind', required=True, choices=list(INPUT_KINDS.keys()),
        help='The format of the input to read')
    parser.add_argument(
        '--output-partition', required=True, type=HivePartitionWriter.from_spec,
        help='Table and partition to write prepared predictions to')
    parser.add_argument(
        '--thresholds-path', dest='thresholds', type=json_path, required=True,
        help='Path to json file containing per-wiki/topic thresholds to apply')
    parser.add_argument(
        '--prediction', required=True,
        help='Name of model to extract predictions of')
    parser.add_argument(
        '--alias', default=None, required=False,
        help='Name of prediction column in output table. Model name will be used if not provided.')
    parser.add_argument(
        '--wikibase-item-partition', required=False, type=HivePartition.from_spec,
        help='Table and partition containing export of wikibase_item '
             'page props from mw replicas.')
    parser.add_argument(
        '--propagate-from', required=False,
        help='Wiki database name to propagate predictions from.')
    # We "know" that the data is relatively small so make a single output
    # partition, with the option to override for larger one-off tasks.
    parser.add_argument(
        '--num-output-partitions', default=1, type=int,
        help='Number of output partitions to create. Estimate at 100MB per partition.')
    return parser


def main(
    input_partition: HivePartitionTimeRange,
    input_kind: str,
    output_partition: HivePartitionWriter,
    # Thresholds is expected to contain a two level dict. top level must be
    # keyed by mediawiki database name, second level must be a mapping
    # from predicted label to minimum acceptable threshold. Unlisted
    # wikis / labels recieve DEFAULT_THRESHOLD
    thresholds: Mapping[str, Mapping[str, float]],
    prediction: str,
    alias: str,
    wikibase_item_partition: Optional[HivePartition],
    propagate_from: Optional[str],
    num_output_partitions: int
) -> int:
    if propagate_from is not None and propagate_from not in thresholds:
        raise Exception('No thresholds provided for propagation wiki, no propagation can occur.')

    spark = SparkSession.builder.getOrCreate()

    df_in = INPUT_KINDS[input_kind](
        input_partition.read(spark), prediction)

    # Find appropriate data, convert into expected formats
    df_predictions = extract_prediction(df_in, prediction, thresholds)

    # The model name may not exactly match the field we index into, provide
    # support rename the exported field.
    if alias is None:
        prediction_col = prediction
    else:
        prediction_col = alias
        df_predictions = df_predictions.withColumnRenamed(prediction, alias)

    # Propagate predictions from wikis we have models to all the other
    # wikis by wikibase_item.
    if propagate_from is not None:
        if wikibase_item_partition is None:
            raise Exception('propagate_from provided without wikibase_item_table')

        df_predictions = propagate_by_wbitem(
            df_predictions,
            wikibase_item_partition.read(spark),
            prediction_col,
            source_wikis=set(thresholds.keys()),
            preferred_wiki=propagate_from)
    elif wikibase_item_partition is not None:
        logging.warning('wikibase_item_table provided without propagate_from, no propagation will occur')

    # Repartition as desired, spark typically has hundreds of partitions but
    # the final outputs may be anywhere from hundreds of MB to dozens of GB
    # depending on the input dataset.
    df_out = df_predictions \
        .repartition(num_output_partitions) \
        .select(
            'wikiid',
            F.col('page_id').cast('int'),
            F.col('page_namespace').cast('int'),
            prediction_col)

    output_partition.overwrite_with(df_out)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
