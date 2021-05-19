'''
    Push queries to relforge

Queries are taken from both backend requests log and from frontend event logs joined on request_id. Relforge's ES is
then fed those objects. Note that there is a discrepancy in volume of backend and frontend logs, which will result in
some unmatched events.


'''

from argparse import ArgumentParser

import json
import logging
import requests
import sys
from typing import Optional, Sequence, Mapping

from wmf_spark import HivePartition

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

INDEX_CONFIG = 'queries_index_settings.json'


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('--search-satisfaction-partition', required=True, type=HivePartition.from_spec)
    parser.add_argument('--cirrus-events-partition', required=True, type=HivePartition.from_spec)
    parser.add_argument('--elastic-host', required=True)
    parser.add_argument('--elastic-port', required=True)
    parser.add_argument('--elastic-index', required=True)
    parser.add_argument('--elastic-template', required=True)
    parser.add_argument('--no-ssl', required=False, dest='use_ssl', action='store_false')
    parser.set_defaults(use_ssl=True)

    return parser


HitsStructType = T.StructType([
    T.StructField('top1', T.StringType(), True),
    T.StructField('top3', T.ArrayType(T.StringType()), False),
    T.StructField('top10', T.ArrayType(T.StringType()), False)
])

SearchReqType = T.StructType([
    T.StructField('hits_returned', T.IntegerType(), False),
    T.StructField('syntax', T.ArrayType(T.StringType()), False),
    T.StructField('first_page', T.BooleanType(), False),
    T.StructField('indices', T.ArrayType(T.StringType()), False)
])


def extract_hits(hits: Sequence[Mapping]) -> Mapping:
    page_titles = list(map(lambda hit: hit['page_title'], hits)) if hits is not None else []
    return {
        'top1': page_titles[0] if len(page_titles) > 0 else None,
        'top3': page_titles[0:3],
        'top10': page_titles[0:10]
    }


def extract_main_search_request(requests: Sequence[Mapping], wiki: str) -> Optional[Mapping]:
    if requests is None or len(requests) == 0:
        return None

    prefix = wiki + '_'

    for request in requests:
        if request['query_type'] != 'full_text':
            continue

        indices: Sequence[str] = request['indices']
        for index in indices:
            if index is not None and (index == wiki or index.startswith(prefix)):
                return {
                    'hits_returned': request['hits_returned'],
                    'syntax': request['syntax'],
                    'first_page': request['hits_offset'] == 0,
                    'indices': request['indices']
                }

    return None


EXTRACT_MAP = {
    'meta.dt': 'dt',
    'event.query': 'query',
    'event.hitsReturned': 'hits_total',
    'useragent.is_bot': 'is_bot',
    'wiki': 'wiki',
    'hits_top.top1': 'hits_top1',
    'hits_top.top3': 'hits_top3',
    'hits_top.top10': 'hits_top10',
    'search_request.hits_returned': 'hits_returned',
    'search_request.syntax': 'syntax',
    'search_request.first_page': 'first_page',
    'search_request.indices': 'indices'
}


def extract_from_joined_dfs(cirrus_search_request_df: DataFrame, search_satisfaction_df: DataFrame) -> DataFrame:
    df_fe = (
        search_satisfaction_df
        .filter(F.col('event.action') == 'searchResultPage')
        .filter(F.col('event.source') == 'fulltext')
        .dropDuplicates(['event'])
        .select(F.col('event'), F.col('useragent'), F.col('wiki'), F.col('meta'))
    )
    df_be = (
        cirrus_search_request_df
        .select(F.col('search_id'), F.col('hits'), F.col('elasticsearch_requests'))
    )
    join_expr = df_fe['event.searchToken'] == df_be['search_id']
    joined_df = df_fe.join(df_be, join_expr, 'left')
    extract_hits_udf = F.udf(extract_hits, HitsStructType)
    extract_main_search_request_udf = F.udf(extract_main_search_request, SearchReqType)
    enhanced_df = (
        joined_df
        .withColumn('hits_top', extract_hits_udf('hits').alias('top'))
        .withColumn('search_request', extract_main_search_request_udf('elasticsearch_requests', 'wiki'))
    )
    select_args = [F.col(key).alias(value) for key, value in EXTRACT_MAP.items()]
    return enhanced_df.select(*select_args)


def initialize_index_template(elastic_host: str, elastic_port: str, elastic_template: str, use_ssl: bool) -> None:
    template_url = ('https://' if use_ssl else "http://") + elastic_host.split(",")[0] + ':' \
        + str(elastic_port) + '/_template/' + elastic_template

    with open(INDEX_CONFIG) as f:
        data = json.load(f)
        requests.put(template_url, json=data)


def main(
        search_satisfaction_partition: HivePartition,
        cirrus_events_partition: HivePartition,
        elastic_host: str,
        elastic_port: str,
        elastic_index: str,
        elastic_template: str,
        use_ssl: bool
) -> int:
    initialize_index_template(elastic_host, elastic_port, elastic_template, use_ssl)
    spark = SparkSession.builder.getOrCreate()
    initial_searchsatisfaction_df = search_satisfaction_partition.read(spark)
    initial_cirrussearch_requests_df = cirrus_events_partition.read(spark)

    final_df = extract_from_joined_dfs(initial_cirrussearch_requests_df, initial_searchsatisfaction_df)
    (
        final_df.write.format('org.elasticsearch.spark.sql')
        .option('es.nodes', elastic_host)
        .option('es.port', elastic_port)
        .option('es.resource', elastic_index)
        .option('es.net.ssl', str(use_ssl).lower())
        # we need to make sure we're using only ports available in analytics network
        .option('es.nodes.discovery', 'false')
        .option('es.nodes.wan.only', 'true')
        .mode('append')
        .save()
    )

    return 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
