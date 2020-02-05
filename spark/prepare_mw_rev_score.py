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
import sys
from typing import Mapping, Sequence

from pyspark.sql import (
    SparkSession, Column, DataFrame, Window,
    functions as F, types as T)


# Probabilities need to be expressed as an integer to integrate into the
# search engine. Scale probabilities by this value before casting to int.
# The must match the max_tf value configured for the term_freq analyzer
# in the elasticsearch field data is indexed into.
SCALE_FACTOR = 1000


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


def stringify_prediction(
    probabilities: Mapping[str, float], threshold: float
) -> Sequence[str]:
    """Format predictions for elasticsearch ingestion.

    To index the probabilities into the search engine we convert the
    probabilities into an integer score and format as `<token>|<score>`. The
    token is the exact searchable value, and the score is interpreted as the
    token term frequency.
    """
    if any('|' in topic for topic in probabilities.keys()):
        raise Exception('Topic names must not contain |')
    return [
        '{}|{}'.format(topic, int(SCALE_FACTOR * prob))
        for topic, prob in probabilities.items()
        if prob >= threshold
    ]


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


def extract_prediction(
    df_in: DataFrame,
    prediction: str,
    threshold: float
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
        stringify_prediction, T.ArrayType(T.StringType()))

    df_converted = df_filtered.select(
        F.col('database').alias('wikiid'),
        F.col('page_id'),
        stringify_prediction_udf(
            F.col('scores')[prediction].probability,
            F.lit(threshold)
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
    parser.add_argument('--input-table', required=True)
    parser.add_argument('--output-table', required=True)
    parser.add_argument('--start-date', type=date, required=True)
    parser.add_argument('--end-date', type=date, required=True)
    parser.add_argument('--threshold', type=float, required=True)
    parser.add_argument('--prediction', required=True)
    parser.add_argument('--alias', default=None, required=False)
    args = parser.parse_args(raw_args)

    spark = SparkSession.builder.getOrCreate()

    df_in = (
        spark.read.table(args.input_table)
        # Limit the input dataset to the time range requested
        .where(hourly_partition_range(args.start_date, args.end_date))
    )

    # Find appropriate data, convert into expected formats
    df_predictions = extract_prediction(df_in, args.prediction, args.threshold)

    # Support transition from drafttopic -> articletopic
    if args.alias is None:
        prediction_col = args.prediction
    else:
        prediction_col = args.alias
        df_predictions = df_predictions.withColumnRenamed(args.prediction, args.alias)

    # We "know" that the data is relatively small so make a single output
    # partition and give it a name we can reference from sql.
    df_predictions.repartition(1).createTempView('tmp_revision_score_out')

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
