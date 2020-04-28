"""Extracts one day of json formatted hourly search_satisfaction to be loaded in Druid."""

from argparse import ArgumentParser
import sys

try:
    from pyspark.sql import SparkSession
except ImportError:
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession

from pyspark.sql import DataFrame, functions as F


# Backport from spark 2.4.0
DataFrame.transform = lambda df, fn: fn(df)  # type: ignore


def arg_parser():
    parser = ArgumentParser()
    parser.add_argument('--source-table', default='discovery.search_satisfaction_daily')
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


def group_by_excluding(*col_names):
    def transform(df):
        group_cols = set(df.columns) - set(col_names)
        return df.groupBy(*list(group_cols))
    return transform


def main(source_table, destination_directory, year, month, day):
    spark = SparkSession.builder.getOrCreate()

    (
        spark.read.table(source_table)
        .where(F.col('year') == year)
        .where(F.col('month') == month)
        .where(F.col('day') == day)
        .drop('searchSessionId')
        # Replace dt with low-precision hourly dt
        .withColumn('dt', F.concat(
            F.substring(F.col('dt'), 0, len('0000-00-00T00:')),
            F.lit('00:00Z')))
        .withColumn('hits_returned', bucketize(F.col('hits_returned'), [
            -1, 0, 1, 2, 5, 10, 20, 50, 100, 1000, 10000, 100000, 1000000
        ]))
        .drop('year', 'month', 'day', 'hour')
        .transform(group_by_excluding('sample_multiplier'))
        .agg(F.count(F.lit(1)).alias('search_count'),
             F.sum(F.col('sample_multiplier')).alias('search_count_norm'))
        # The daily output is tiny, no need for a bunch of partitions
        .repartition(1)
        .write
        .option('compression', 'gzip')
        .json(destination_directory)
    )


if __name__ == "__main__":
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
