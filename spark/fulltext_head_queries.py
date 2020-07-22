"""Generate a dataset of head queries for each wiki

Reports the number of unique search sessions performing the same normalized
query over the lifetime of the input table. No filtering of heavy users is
applied, but since SearchSatisfaction events come from javascript bots have
less of an effect.  Additionally the context of the search, particularly the
set of namespaces searched within, is not taken into account. Queries performed
against different subsets of the content are counted together.

Queries normalized for aggregation as follows:

* lowercase
* Replace all punctuation and whitespace with ' '
* squeeze all sequential spaces into a single space

This is over aggressive for full punctuation queries, but otherwise does a
reasonable job of capturing query intent. The exact queries issued along with
the number of times each was seen is reported in the queries column.
"""
from argparse import ArgumentParser
from collections import Counter
import logging
from pyspark.sql import (
    Column, DataFrame, SparkSession, Window,
    functions as F, types as T)
from typing import Sequence, Tuple

from wmf_spark import HivePartition, HivePartitionWriter


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        '--search-satisfaction-partition', default='event.searchsatisfaction/', type=HivePartition.from_spec,
        help='Eventbus table containing SearchSatisfaction events')
    parser.add_argument(
        '--num-queries', default=10000, type=int,
        help='The number of queries to report per wiki')
    parser.add_argument(
        '--output-partition', required=True, type=HivePartitionWriter.from_spec,
        help='Output partition in hive partition format: mydb.mytable/k1=v1/k2=v2')
    return parser


def cleanup_separators(val: Column) -> Column:
    # Replace all space-like and punctuation with spaces
    val = F.regexp_replace(val, r'[\s\p{Punct}]', ' ')
    # Deduplicate repeated spaces
    val = F.regexp_replace(val, r' +', ' ')
    # Strip leading and trailing spaces
    val = F.trim(val)
    return val


def norm_query(query: Column) -> Column:
    """Perform basic normalization of strings

    Goal is to give strings with the same textual intent the same output
    string. In this case that mostly means converting non-characters into
    spaces, flattening duplicate spaces, and striping spaces from the beginning
    and end.  Ideally this should come from elsewhere and not be implemented in
    this script.
    """
    query = F.lower(query)
    query = cleanup_separators(query)
    return query


def count_duplicates(strings: Sequence[str]) -> Sequence[Tuple[str, int]]:
    counts = Counter(strings).items()
    # Sort in descending order for human readability
    return list(sorted(counts, key=lambda x: x[1], reverse=True))


def extract_head_queries(
    df: DataFrame,
    num_queries: int,
    min_sessions: int = 2
) -> DataFrame:
    count_duplicates_udf = F.udf(count_duplicates, T.ArrayType(T.StructType([
        T.StructField('query', T.StringType()),
        T.StructField('num_sessions', T.IntegerType()),
    ])))

    return (
        df
        # Narrow in on full text events for performing a search
        .where(F.col('event.source') == 'fulltext')
        .where(F.col('event.action') == 'searchResultPage')
        # Further narrow in on the only data we care about
        .select('wiki', 'event.searchSessionId', 'event.query')
        # Cleanup source queries into something slightly more comparable
        .withColumn('norm_query', norm_query(F.col('query')))
        # Only count a query performed by a session one time
        .drop_duplicates(['searchSessionId', 'norm_query'])
        # Count up the number of sessions. Queries is collected
        # primarily to inform manual analysis of grouping.
        .groupBy('wiki', 'norm_query')
        # count returns a Long, but we know it fits in 32 bits.
        .agg(F.count(F.lit(1)).cast(T.IntegerType()).alias('num_sessions'),
             F.collect_list(F.col('query')).alias('queries'))
        .withColumn('queries', count_duplicates_udf(F.col('queries')))
        # row_number() has to bring all data for one wiki to
        # a single partition, dropping single session queries
        # reduces the data significantly.
        .where(F.col('num_sessions') >= min_sessions)
        .withColumn('rank', F.row_number().over(
            Window.partitionBy('wiki')
            .orderBy(F.col('num_sessions').desc())))
        .where(F.col('rank') <= num_queries)  # rank is 1-indexed
    )


def main(
    search_satisfaction_partition: HivePartition,
    num_queries: int,
    output_partition: HivePartitionWriter,
) -> int:
    spark = SparkSession.builder.getOrCreate()
    df_in = search_satisfaction_partition.read(spark)
    df_out = extract_head_queries(df_in, num_queries) \
        .repartition(10)
    output_partition.overwrite_with(df_out)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    main(**dict(vars(args)))
