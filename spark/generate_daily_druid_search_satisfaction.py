"""Extracts one day of json formatted hourly search_satisfaction to be loaded in Druid."""

from argparse import ArgumentParser
import logging
import sys

try:
    from pyspark.sql import SparkSession
except ImportError:
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession

from pyspark.sql import DataFrame, functions as F
from wmf_spark import HivePartition


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        '--source-partition', default='discovery.search_satisfaction_daily',
        type=HivePartition.from_spec)
    parser.add_argument('--destination-directory', required=True)
    parser.add_argument('--year', required=True, type=int)
    parser.add_argument('--month', required=True, type=int)
    parser.add_argument('--day', required=True, type=int)
    return parser


def bucketize(source_col, buckets):
    assert len(buckets) > 0
    min_val = None
    bucket_fmt = '{: ' + str(len(str(buckets[-1]))) + 'd}'
    agg_cond = F.when(source_col.isNull(), F.lit(None))
    for bucket in buckets:
        if min_val is None:
            cond = source_col <= bucket
            desc = '<= {}'.format(bucket_fmt.format(bucket))
        elif min_val == bucket:
            cond = source_col == bucket
            desc = str(bucket)
        else:
            cond = source_col.between(min_val, bucket)
            desc = '{}-{}'.format(bucket_fmt.format(min_val), bucket_fmt.format(bucket))
        agg_cond = agg_cond.when(cond, F.lit(desc))
        min_val = bucket + 1
    return agg_cond.otherwise(F.lit('{}+'.format(min_val)))


def transform(df_source: DataFrame) -> DataFrame:
    """Transform search satisfaction dataset into druid dataset

    Reduces precision of dataset to satisfy druid's need for
    low cardinality data. Pre-aggregates over remaining columns
    """
    df = (
        df_source
        # Remove columns we don't want to aggregate over later
        .drop('year', 'month', 'day', 'hour', 'searchSessionId')
        # Replace dt with low-precision hourly dt
        .withColumn('dt', F.concat(
            F.substring(F.col('dt'), 0, len('0000-00-00T00:')),
            F.lit('00:00Z')))
        # Replace hits with range buckets
        .withColumn('hits_returned', bucketize(F.col('hits_returned'), [
            -1, 0, 1, 2, 5, 10, 20, 50, 100, 1000, 10000, 100000, 1000000
        ]))
    )

    return (
        df
        # All values should now be low cardinality, take search
        # session counts over all fields.
        .groupBy(*list(set(df.columns) - {'sample_multiplier'}))
        .agg(F.count(F.lit(1)).alias('search_count'),
             F.sum(F.col('sample_multiplier')).alias('search_count_norm'))
    )


def main(
    source_partition: HivePartition,
    destination_directory: str,
) -> int:
    spark = SparkSession.builder.getOrCreate()

    (
        transform(source_partition.read(spark))
        # The daily output is tiny, no need for a bunch of partitions
        .repartition(1)
        .write
        .option('compression', 'gzip')
        .json(destination_directory)
    )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
