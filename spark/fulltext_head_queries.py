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
from pyspark.sql import (
    Column, DataFrame, SparkSession, Window,
    functions as F, types as T)
from typing import Mapping, Optional, Sequence, Tuple


def arg_parser():
    parser = ArgumentParser()
    parser.add_argument(
        '--search-satisfaction-table', default='event.searchsatisfaction',
        help='Eventbus table containing SearchSatisfaction events')
    parser.add_argument(
        '--num-queries', default=10000, type=int,
        help='The number of queries to report per wiki')
    parser.add_argument('--output-partition', required=True)
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


def parse_partition_spec(spec: str) -> Tuple[str, Optional[Mapping[str, str]]]:
    if '/' not in spec:
        return spec, None
    pieces = spec.split('/')
    table_name = pieces[0]
    partitions = dict(kv_pair.split('=', 1) for kv_pair in pieces[1:])  # type: ignore
    return table_name, partitions


def insert_into(df: DataFrame, spec: str) -> None:
    table_name, partitioning = parse_partition_spec(spec)
    if partitioning is None:
        raise Exception('Invalid partition spec, no partitioning provided')
    for k, v in partitioning.items():
        if k in df.columns:
            raise Exception('Partition key {} overwriting dataframe column'.format(k))
        df = df.withColumn(k, F.lit(v))

    # In testing spark put values in the wrong columns of the table unless
    # we made the order exactly the same.
    columns = df.sql_ctx.read.table(table_name).columns
    if set(columns) != set(df.columns):
        raise Exception(
            'Mismatched columns, provided {} but found {} in target table'.format(
                list(columns), list(df.columns)))

    # insertInto is position based insert, align with loaded schema
    df = df.select(*columns)

    # Required for insert_into to only overwrite the partitions being
    # written to and not the whole table.
    df.sql_ctx.setConf('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    # Required for insert_into to perform partition selection on per-row
    # basis. We can't specify anything more strict from spark. Also there
    # is no way to tell spark we are writing a single partition, we have to
    # let it choose per-row.
    df.sql_ctx.setConf('hive.exec.dynamic.partition.mode', 'nonstrict')
    # Testing showed .mode('overwrite') to be ignored, this only appropriately
    # clears existing partitions when passing overwrite=True directly to the
    # insertInto method.
    df.write.insertInto(table_name, overwrite=True)


def main(
    search_satisfaction_table: str,
    num_queries: int, output_partition: str,
):
    spark = (
        SparkSession.builder
        .getOrCreate()
    )

    df_in = spark.read.table(search_satisfaction_table)

    df_out = extract_head_queries(df_in, num_queries) \
        .repartition(10)

    insert_into(df_out, output_partition)


if __name__ == "__main__":
    args = arg_parser().parse_args()
    main(**dict(vars(args)))
