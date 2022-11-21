from argparse import ArgumentParser
import logging
from pyspark.sql import Column, DataFrame, SparkSession, functions as F
from wmf_spark import HivePartition, HivePartitionWriter


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        '--cirrus-dump', required=True, type=HivePartition.from_spec,
        help='Input partition of cirrus index dump in hive partition format.')
    parser.add_argument(
        '--output-partition', required=True, type=HivePartitionWriter.from_spec,
        help='Output partition in hive partition format: mydb.mytable/k1=v1/k2=v2')
    return parser


def dbkey() -> Column:
    needs_ns = F.length(F.col('namespace_text')) > 0
    ns_title = F.concat_ws(':', F.col('namespace_text'), F.col('title'))
    full_title = F.when(needs_ns, ns_title).otherwise(F.col('title'))
    return F.regexp_replace(full_title, r' ', '_').alias('dbkey')


def calc_incoming_links(df: DataFrame) -> DataFrame:
    df_agg = (
        df
        .select(df.wiki, F.explode(df.outgoing_link).alias('outgoing_link'))
        .groupBy(df.wiki, F.col('outgoing_link').alias('dbkey'))
        .agg(F.count(F.lit(1)).alias('incoming_links')))

    return (
        df.select(
            df.wiki,
            df.page_id,
            df.namespace.alias('page_namespace'),
            df.incoming_links.alias('old_incoming_links'),
            dbkey())
        .join(df_agg, ['wiki', 'dbkey'], how='left')
        # Pages with no incoming links are in df but not df_agg, set those to 0
        .withColumn('incoming_links', F.coalesce(F.col('incoming_links'), F.lit(0)))
        # Only store pages that differ. If retained they would still be elided
        # at the elasticsearch level by super_detect_noop, but this avoids
        # sending 150M+ pages that didn't change since the previous snapshot.
        .where(F.col('old_incoming_links') != F.col('incoming_links'))
        .drop(F.col('old_incoming_links'))
        # Match expected column names in convert_to_esbulk.py
        .withColumnRenamed('wiki', 'wikiid')
    )


def main(
    cirrus_dump: HivePartition,
    output_partition: HivePartitionWriter,
):
    spark = SparkSession.builder.getOrCreate()
    df_in = cirrus_dump.read(spark)
    df_out = calc_incoming_links(df_in)
    output_partition.overwrite_with(df_out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    main(**dict(vars(args)))
