# Generates and inserts data for the event.mediawiki_cirrussearch_request table
#
# Supported pipelines:
#  ores_predictions_weekly
#
# Each gen_* method generates every possible field to ensure compatability. Fields
# are all in schema order. Most fields are either null'd or defaulted to simplify
# the datasets needed for our specific processing.
#

from pyspark.sql import SparkSession, Row, functions as F, types as T
import sys
from typing import Mapping, Sequence

from wmf_spark import HivePartitionWriter

def gen_elastic_req(
    query: str,
    query_type: str,
    namespaces: Sequence[int],
    indices: Sequence[str],
    syntax: Sequence[str]
) -> Mapping:
    assert len(indices) > 0, \
        "We write to parquet, and parquet doesn't support empty arrays. SPARK-25271"

    return dict(
        query=query,
        query_type=query_type,
        indices=list(indices),
        namespaces=list(namespaces),
        request_time_ms=None,
        search_time_ms=None,
        limit=None,
        hits_total=None,
        hits_returned=None,
        hits_offset=None,
        suggestion=None,
        suggestion_requested=None,
        max_score=None,
        langdetect=None,
        syntax=syntax,
        cached=None,
        hits=None,
    )


def gen_row(search_id: str, params: Mapping[str, str], source: str, identity: str, database: str, elastic_reqs: Sequence[Mapping]) -> Mapping:
    # we write to parquet, and parquet doesnt support empty arrays
    assert len(elastic_reqs) > 0, \
        "We write to parquet, and parquet doesn't support empty arrays. SPARK-25271"

    return dict(
        _schema=None,
        meta=None,
        http=None,
        database=database,
        mediawiki_host=None, # Could load canonical_data.wikis and map to proper value
        params=params,
        search_id=search_id,
        source=source,
        identity=identity,
        backend_user_tests=None,
        request_time_ms=None,
        hits=None,
        all_elasticsearch_requests_cached=False,
        elasticsearch_requests=[gen_elastic_req(**req) for req in elastic_reqs],
        geocoded_data=None,
        user_agent_map=None,
    )


def main():
    spark = SparkSession.builder.getOrCreate()
    table = 'event.mediawiki_cirrussearch_request'
    df = spark.read.table(table)

    assert df.count() == 0, "Expect table to be empty"

    row_spec = [
        {
            "search_id": "abc123",
            "params": {
                'search': 'boats',
            },
            "source": "web",
            "identity": "A",
            "database": "testwiki",
            "elastic_reqs": [
                {
                    'query': 'boats',
                    'query_type': 'full_text',
                    'namespaces': [0],
                    'indices': ['testwiki_content'],
                    'syntax': ['full_text', 'full_text_simple_match', 'simple_bag_of_words']
                }
            ]
        }
    ]

    row_schema = df.drop('datacenter', 'year', 'month', 'day', 'hour').schema
    rows = [gen_row(**row) for row in row_spec]

    partition = HivePartitionWriter.from_spec(
        '{}/datacenter=eqiad/year=2001/month=1/day=15/hour=19'.format(table))
    # By using createDataFrame with a schema and a list of dicts we ensure
    # everything in the correct order for insertion, as it will fetch
    # everything by name, but extra values will be ignored and missing
    # keys will silently be set to null.
    partition.overwrite_with(spark.createDataFrame(rows, row_schema))


if __name__ == "__main__":
    main()
