"""
Prepare recommendations from recommendation-create for elasticsearch ingestion

Input table must match the /mediawiki/revision/recommendatoin-create/1.0.0 schema
"""
from argparse import ArgumentParser
import logging
import sys

from pyspark.sql import SparkSession, DataFrame, Window, functions as F
from wmf_spark import HivePartitionTimeRange, HivePartitionWriter, limit_top_n


def extract_prediction(df: DataFrame) -> DataFrame:
    """Extract and format predictions from the shared input schema"""
    return (
        # Only take the most recent revision per page per recommendation type
        limit_top_n(
            df,
            Window.partitionBy('database', 'page_id', 'recommendation_type').orderBy(F.col('rev_id').desc()),
            n=1)
        # Simplify down to only the information we need. Cast integers
        # to ensure they match our output types.
        .select(
            F.col('database').alias('wikiid'),
            F.col('page_id').cast('int'),
            F.col('page_namespace').cast('int'),
            F.col('recommendation_type'))
    )


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        '--input-partition', required=True, type=HivePartitionTimeRange.from_spec,
        help='Specification for input partitions.')
    parser.add_argument(
        '--output-partition', required=True, type=HivePartitionWriter.from_spec,
        help='Table and partition to write prepared recommendations to')
    parser.add_argument(
        '--num-output-partitions', default=1, type=int,
        help='Number of output partitions to create. Estimate at 100MB per partition.')
    return parser


def main(
    input_partition: HivePartitionTimeRange,
    output_partition: HivePartitionWriter,
    num_output_partitions: int
) -> int:
    spark = SparkSession.builder.getOrCreate()

    df_predictions = extract_prediction(input_partition.read(spark))
    # TODO: num_output_partitions should be optional part of output_partition?
    output_partition.overwrite_with(df_predictions.repartition(num_output_partitions))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
