"""
Prepare predictions from revision score events for elasticsearch ingestion

Input table is expected to contain rows conforming to the
mediawiki/revision/score jsonschema from mediawiki/event-schemas repository.
Table is expected to be partitioned hourly with year/month/day/hour partition
keys. Output table is structured to be passed on to convert_to_esbulk.py and
shipped to elasticsearch.
"""
from argparse import ArgumentParser
from datetime import datetime
import json
import sys
from typing import cast, Callable, Mapping, Optional, Sequence, Set

from pyspark.sql import (
    SparkSession, Column, DataFrame, Row, Window,
    functions as F, types as T)


# Probabilities need to be expressed as an integer to integrate into the
# search engine. Scale probabilities by this value before casting to int.
# This must match the max_tf value configured for the term_freq analyzer
# in the elasticsearch field data is indexed into.
SCALE_FACTOR = 1000

# If no threshold is available the prediction must have at least
# this value to be emitted
DEFAULT_THRESHOLD = 0.9


def hourly_partition_range(start_dt: datetime, end_dt: datetime) -> Column:
    """Boolean condition for hourly partitions within period"""
    row_date_str = F.concat(
        F.col('year'), F.lit('-'),
        F.lpad(F.col('month'), 2, '0'), F.lit('-'),
        F.lpad(F.col('day'), 2, '0'), F.lit(' '),
        F.lpad(F.col('hour'), 2, '0'), F.lit(':00:00'))
    row_date = F.unix_timestamp(row_date_str, 'yyyy-MM-dd HH:mm:ss')

    start_cond = row_date >= start_dt.timestamp()
    end_cond = row_date < end_dt.timestamp()
    return start_cond & end_cond


def daily_partition(dt: datetime) -> Column:
    """Boolean condition for single daily partition"""
    return (F.col('year') == dt.year) \
        & (F.col('month') == dt.month) \
        & (F.col('day') == dt.day)


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
        out = [(p.wikiid, p.page_id, p[prediction_col]) for p in predictions]

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
                out.append((page.wikiid, page.page_id, preferred))

        return out

    # Ensure we have the set of columns we think we do
    df_wikibase = df_wikibase.select('wikiid', 'page_id', 'wikibase_item')
    df_predictions = df_predictions.select('wikiid', 'page_id', prediction_col)

    df_pages_by_link = (
        df_wikibase
        .groupBy('wikibase_item')
        .agg(F.collect_list(F.struct('wikiid', 'page_id')).alias('pages'))
    )

    df_predictions_by_link = (
        df_predictions
        .join(df_wikibase, how='left', on=['wikiid', 'page_id'])
        # Not every prediction will have a wikibase_item, but we don't want
        # to pull them into a single giant row. Assign fake wikibase_item strings
        # that are unique per prediction to avoid skew.
        .withColumn('wikibase_item', F.coalesce(
            F.col('wikibase_item'),
            F.concat_ws('-', F.lit('notfound'), F.col('wikiid'), F.col('page_id').cast(T.StringType()))))
        .groupBy('wikibase_item')
        .agg(F.collect_list(F.struct(
            'wikiid', 'page_id', prediction_col
        )).alias('predictions'))
    )

    propagate_udf = F.udf(
        resolve_propagation,
        T.ArrayType(T.StructType([
            T.StructField('wikiid', T.StringType()),
            T.StructField('page_id', T.IntegerType()),
            T.StructField(prediction_col, T.ArrayType(T.StringType()))
        ])))

    return (
        df_predictions_by_link
        .join(df_pages_by_link, how='left', on=['wikibase_item'])
        .select(F.explode(propagate_udf('predictions', 'pages')).alias('p'))
        .select('p.*')
    )


def extract_prediction(
    df_in: DataFrame,
    prediction: str,
    thresholds: Mapping[str, Mapping[str, float]],
) -> DataFrame:
    df_in = df_in.where(F.col('scores')[prediction].isNotNull())

    # A given page may have multiple predictions in the time
    # span. Keep only the most recent prediction per page
    df_filtered = top_row_per_group(
        df_in,
        [F.col('database'), F.col('page_id')],
        [F.col('rev_timestamp').desc()])

    # Reshape for shipping to elasticesarch
    stringify_prediction_udf = F.udf(
        make_stringify_prediction(thresholds),
        T.ArrayType(T.StringType()))

    df_converted = df_filtered.select(
        F.col('database').alias('wikiid'),
        F.col('page_id'),
        stringify_prediction_udf(
            F.col('database'),
            F.col('scores')[prediction].probability
        ).alias(prediction))

    # Writing out empty arrays fails with parquet output, looks
    # similar to Spark-25271 (resolved in spark 3). For now drop
    # the empty arrays. This has the downside that we have no
    # way to clear previously indexed predictions, we can't send
    # an empty array and have the previous set replaced.
    return df_converted.where(F.size(F.col(prediction)) > 0)


def main(raw_args: Sequence[str]) -> int:
    def date(val: str) -> datetime:
        return datetime.strptime(val, '%Y-%m-%d')

    parser = ArgumentParser()
    parser.add_argument(
        '--input-table', required=True,
        help='Table containing mediawiki/revision/score events')
    parser.add_argument(
        '--output-table', required=True,
        help='Table to write prepared predictions to')
    parser.add_argument(
        '--start-date', type=date, required=True,
        help='Inclusive date to start reading input from')
    parser.add_argument(
        '--end-date', type=date, required=True,
        help='Exclusive date to stop reading input from')
    parser.add_argument(
        '--thresholds-path', type=str, required=True,
        help='Path to json file containing per-wiki/topic thresholds to apply')
    parser.add_argument(
        '--prediction', required=True,
        help='Name of model to extract predictions of')
    parser.add_argument(
        '--alias', default=None, required=False,
        help='Name of prediction column in output table. Model name will be used if not provided.')
    parser.add_argument(
        '--wikibase-item-table', required=True,
        help='Table containing export of wikibase_item page props from mw replicas. '
             'The partition matching start-date will be read.')
    parser.add_argument(
        '--propagate-from', required=True,
        help='Wiki database name to propagate predictions from.')
    # We "know" that the data is relatively small so make a single output
    # partition, with the option to override for larger one-off tasks.
    parser.add_argument(
        '--num-output-partitions', default=1,
        help='Number of output partitions to create. Estimate at 100MB per partition.')

    args = parser.parse_args(raw_args)

    # Path is expected to contain a two level dict. top level must be
    # keyed by mediawiki database name, second level must be a mapping
    # from predicted label to minimum acceptable threshold. Unlisted
    # wikis / labels recieve DEFAULT_THRESHOLD
    with open(args.thresholds_path, 'rt') as f:
        thresholds = json.load(f)

    if args.propagate_from not in thresholds:
        raise KeyError('No thresholds provided for propagation wiki, no propagation can occur.')

    spark = SparkSession.builder.getOrCreate()

    df_in = (
        spark.read.table(args.input_table)
        # Limit the input dataset to the time range requested
        .where(hourly_partition_range(args.start_date, args.end_date))
    )

    # Find appropriate data, convert into expected formats
    df_predictions = extract_prediction(df_in, args.prediction, thresholds)

    # Support transition from drafttopic -> articletopic
    if args.alias is None:
        prediction_col = args.prediction
    else:
        prediction_col = args.alias
        df_predictions = df_predictions.withColumnRenamed(args.prediction, args.alias)

    # Propagate predictions from wikis we have models to all the other
    # wikis by wikibase_item.
    df_wikibase_item = (
        spark.read.table(args.wikibase_item_table)
        .where(daily_partition(args.start_date))
    )

    df_propagated = propagate_by_wbitem(
        df_predictions,
        df_wikibase_item,
        args.prediction,
        source_wikis=set(thresholds.keys()),
        preferred_wiki=args.propagate_from)

    # Repartition as desired, spark typically has hundreds of partitions but the
    # final outputs may only be a few hundred mb.
    df_propagated \
        .repartition(args.num_output_partitions) \
        .createTempView('tmp_revision_score_out')

    # Take care that selected columns must be of the same types and in the same
    # order as the table. The names used here are not used to align columns with
    # the table, it is strictly order-based.
    insert_stmt = """
        INSERT OVERWRITE TABLE {table}
        PARTITION(year={year}, month={month}, day={day})
        SELECT wikiid, CAST(page_id AS int), {prediction}
        FROM tmp_revision_score_out
    """.format(
        table=args.output_table,
        year=args.start_date.year,
        month=args.start_date.month,
        day=args.start_date.day,
        prediction=prediction_col)

    spark.sql(insert_stmt)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
