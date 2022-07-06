""" Subgraph Mapping Init Dag
      Create table and partitions for subgraph_mapping_weekly dag
    Subgraph Mapping Dag
      Maps triples and items to top subgraphs.
      Saves list of all subgraphs along with the mappings in tables.
      Runs every monday to match with the wikidata dumps data.
    Subgraph Query Mapping Init Dag
      Create table and partitions for subgraph_query_mapping_daily dag
    Subgraph Query Mapping Dag
      Maps all WDQS sparql queries to one or more subgraphs. The queries
      are said to access the mapped subgraphs.
      Saves query-subgraph mapping table and 3 more tables that list the
      reason of the match.
      Runs daily.
"""
from datetime import datetime, timedelta

import pendulum
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from wmf_airflow import DAG
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import (DagConf, wmf_conf, YMD_PARTITION, WDQS_SPARK_TOOLS)

dag_conf = DagConf('subgraph_and_query_mapping_conf')

WIKIDATA_TABLE = dag_conf('wikidata_dumps_table')
ALL_SUBGRAPHS_TABLE = dag_conf('all_subgraphs_table')
TOP_SUBGRAPH_ITEMS_TABLE = dag_conf('top_subgraph_items_table')
TOP_SUBGRAPH_TRIPLES_TABLE = dag_conf('top_subgraph_triples_table')
PROCESSED_QUERY_TABLE = dag_conf('processed_query_table')
SUBGRAPH_QITEM_MATCH_TABLE = dag_conf('subgraph_qitem_match_table')
SUBGRAPH_PREDICATE_MATCH_TABLE = dag_conf('subgraph_predicate_match_table')
SUBGRAPH_URI_MATCH_TABLE = dag_conf('subgraph_uri_match_table')
SUBGRAPH_QUERY_MAPPING_TABLE = dag_conf('subgraph_query_mapping_table')
MIN_ITEMS = dag_conf('min_items')
FILTERING_LIMIT = dag_conf('filtering_limit')
WIKI = dag_conf('wiki')

# Default kwargs for all Operators
default_args = {
    'start_date': datetime(2022, 6, 20),
}

with DAG(
        'subgraph_mapping_init',
        default_args=default_args,
        schedule_interval='@once',
        user_defined_macros={
            'dag_conf': dag_conf.macro,
            'wmf_conf': wmf_conf.macro,
        },
) as subgraph_mapping_dag_init:
    complete = DummyOperator(task_id='complete')
    HiveOperator(
        task_id='create_tables',
        hql="""
            CREATE TABLE IF NOT EXISTS {{ dag_conf.all_subgraphs_table }} (
                `subgraph`                    string  COMMENT 'URI of subgraphs in wikidata',
                `count`                       string  COMMENT 'Number of items in the subgraph'
            )
            PARTITIONED BY (
                `snapshot` string,
                `wiki` string
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_all_subgraphs_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.top_subgraph_items_table }} (
                `subgraph`                    string  COMMENT 'URI of subgraphs in wikidata',
                `item`                        string  COMMENT 'Item belonging to corresponding subgraph'
            )
            PARTITIONED BY (
                `snapshot` string,
                `wiki` string
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_top_subgraph_items_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.top_subgraph_triples_table }} (
                `subgraph`                    string  COMMENT 'URI of subgraphs in wikidata',
                `item`                        string  COMMENT 'Item belonging to corresponding subgraph',
                `subject`                     string  COMMENT 'Subject of the triple',
                `predicate`                   string  COMMENT 'Predicate of the triple',
                `object`                      string  COMMENT 'Object of the triple',
                `predicate_code`              string  COMMENT 'Last suffix of the predicate of the triple (i.e P123, rdf-schema#label etc)'
            )
            PARTITIONED BY (
                `snapshot` string,
                `wiki` string
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_top_subgraph_triples_location }}'
            ;
            """  # noqa
    ) >> complete

with DAG(
        'subgraph_mapping_weekly',
        default_args=default_args,
        # wikidata dumps are available every monday ~6 UTC
        # schedule this dag for monday to keep the dates in sync
        schedule_interval='0 6 * * 1',
        # As a weekly job there should never really be more than
        # one running at a time.
        max_active_runs=1,
        catchup=True,
) as subgraph_mapping_dag:
    date = "{{ execution_date.format('%Y%m%d') }}"
    wikidata_table_and_partition: str = '%s/date=%s/wiki=%s' % (WIKIDATA_TABLE, date, WIKI)

    wait_for_data = NamedHivePartitionSensor(
        task_id='wait_for_data',
        sla=timedelta(days=1),
        retries=4,
        partition_names=[wikidata_table_and_partition],
    )

    map_subgraphs = SparkSubmitOperator(
        task_id='map_subgraphs',
        conf={
            # Delegate retries to airflow
            'spark.yarn.maxAppAttempts': '1',
        },
        application=WDQS_SPARK_TOOLS,
        pool='sequential',
        java_class="org.wikidata.query.rdf.spark.transform.structureddata.subgraphs.SubgraphMappingLauncher", # noqa
        max_executors=64,
        executor_cores=4,
        executor_memory="12g",
        driver_memory="8g",
        application_args=[
            '--wikidata-table', wikidata_table_and_partition,
            '--all-subgraphs-table', '%s/snapshot=%s/wiki=%s' % (ALL_SUBGRAPHS_TABLE, date, WIKI),
            '--top-subgraph-items-table',
            '%s/snapshot=%s/wiki=%s' % (TOP_SUBGRAPH_ITEMS_TABLE, date, WIKI),
            '--top-subgraph-triples-table',
            '%s/snapshot=%s/wiki=%s' % (TOP_SUBGRAPH_TRIPLES_TABLE, date, WIKI),
            '--min-items', MIN_ITEMS
        ]
    )

    complete = DummyOperator(task_id='complete')

    wait_for_data >> map_subgraphs >> complete

with DAG(
        'subgraph_query_mapping_init',
        default_args=default_args,
        schedule_interval='@once',
        user_defined_macros={
            'dag_conf': dag_conf.macro,
            'wmf_conf': wmf_conf.macro,
        },
) as subgraph_query_mapping_dag_init:
    complete = DummyOperator(task_id='complete')
    HiveOperator(
        task_id='create_tables',
        hql="""
            CREATE TABLE IF NOT EXISTS {{ dag_conf.subgraph_query_mapping_table }} (
                `id`                string   COMMENT 'ID of the SPARQL query',
                `subgraph`          string   COMMENT 'URI of the subgraph the query accesses',
                `qid`               boolean  COMMENT 'Whether the subgraph-query match was through the subgraphs Qid',
                `item`              boolean  COMMENT 'Whether the subgraph-query match was through an item',
                `predicate`         boolean  COMMENT 'Whether the subgraph-query match was through a predicate',
                `uri`               boolean  COMMENT 'Whether the subgraph-query match was through a URI',
                `literal`           boolean  COMMENT 'Whether the subgraph-query match was through a literal'
            )
            PARTITIONED BY (
                `year`              int     COMMENT 'Unpadded year of queries',
                `month`             int     COMMENT 'Unpadded month of queries',
                `day`               int     COMMENT 'Unpadded day of queries',
                `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_subgraph_query_mapping_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.subgraph_qitem_match_table }} (
                `id`                          string  COMMENT 'ID of the SPARQL query',
                `subgraph`                    string  COMMENT 'URI of the subgraph the query accesses',
                `item`                        string  COMMENT 'Item match that caused the query to match with the subgraph'
            )
            PARTITIONED BY (
                `year`              int     COMMENT 'Unpadded year of queries',
                `month`             int     COMMENT 'Unpadded month of queries',
                `day`               int     COMMENT 'Unpadded day of queries',
                `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_subgraph_qitem_match_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.subgraph_predicate_match_table }} (
                `id`                          string  COMMENT 'ID of the SPARQL query',
                `subgraph`                    string  COMMENT 'URI of the subgraph the query accesses',
                `predicate_code`              string  COMMENT 'Wikidata predicates present in queries that are part of the subgraph (causing the match)'
            )
            PARTITIONED BY (
                `year`              int     COMMENT 'Unpadded year of queries',
                `month`             int     COMMENT 'Unpadded month of queries',
                `day`               int     COMMENT 'Unpadded day of queries',
                `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_subgraph_predicate_match_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.subgraph_uri_match_table }} (
                `id`                          string  COMMENT 'ID of the SPARQL query',
                `subgraph`                    string  COMMENT 'URI of the subgraph the query accesses',
                `uri`                         string  COMMENT 'URIs present in queries that are part of the subgraph (causing the match)'
            )
            PARTITIONED BY (
                `year`              int     COMMENT 'Unpadded year of queries',
                `month`             int     COMMENT 'Unpadded month of queries',
                `day`               int     COMMENT 'Unpadded day of queries',
                `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_subgraph_uri_match_location }}'
            ;
            """  # noqa
    ) >> complete

with DAG(
        'subgraph_query_mapping_daily',
        default_args=default_args,
        schedule_interval='@daily',
        max_active_runs=4,
        catchup=True,
        user_defined_macros={
            'p': pendulum,
        },
) as subgraph_query_mapping_dag:
    last_monday = "{{ execution_date.previous(day_of_week=p.MONDAY).format('%Y%m%d') }}"
    last_wikidata_table_and_partition: str = '%s/date=%s/wiki=%s' % (
        WIKIDATA_TABLE, last_monday, WIKI)
    top_subgraph_items_table_and_partition: str = '%s/snapshot=%s/wiki=%s' % (
        TOP_SUBGRAPH_ITEMS_TABLE, last_monday, WIKI)
    top_subgraph_triples_table_and_partition: str = '%s/snapshot=%s/wiki=%s' % (
        TOP_SUBGRAPH_TRIPLES_TABLE, last_monday, WIKI)
    processed_external_sparql_query_table_and_partition: str = '%s/%s/wiki=%s' % (
        PROCESSED_QUERY_TABLE, YMD_PARTITION, WIKI)

    wait_for_data = NamedHivePartitionSensor(
        task_id='wait_for_data',
        sla=timedelta(days=1),
        retries=4,
        partition_names=[last_wikidata_table_and_partition,
                         top_subgraph_items_table_and_partition,
                         top_subgraph_triples_table_and_partition,
                         processed_external_sparql_query_table_and_partition],
    )

    map_subgraphs_queries = SparkSubmitOperator(
        task_id='map_subgraphs_queries',
        conf={
            # Delegate retries to airflow
            'spark.yarn.maxAppAttempts': '1',
        },
        application=WDQS_SPARK_TOOLS,
        pool='sequential',
        java_class="org.wikidata.query.rdf.spark.transform.queries.subgraphsqueries.SubgraphQueryMappingLauncher", # noqa
        max_executors=128,  # takes ~40 mins with 128 max executors and ~2 hrs with 64 max executors
        executor_cores=4,
        executor_memory="8g",
        driver_memory="8g",
        application_args=[
            '--wikidata-table', last_wikidata_table_and_partition,
            '--top-subgraph-items-table', top_subgraph_items_table_and_partition,
            '--top-subgraph-triples-table', top_subgraph_triples_table_and_partition,
            '--processed-query-table', processed_external_sparql_query_table_and_partition,
            '--subgraph-qitem-match-query-table',
            '%s/%s/wiki=%s' % (SUBGRAPH_QITEM_MATCH_TABLE, YMD_PARTITION, WIKI),
            '--subgraph-predicate-match-query-table',
            '%s/%s/wiki=%s' % (SUBGRAPH_PREDICATE_MATCH_TABLE, YMD_PARTITION, WIKI),
            '--subgraph-uri-match-query-table',
            '%s/%s/wiki=%s' % (SUBGRAPH_URI_MATCH_TABLE, YMD_PARTITION, WIKI),
            '--subgraph-query-mapping-table',
            '%s/%s/wiki=%s' % (SUBGRAPH_QUERY_MAPPING_TABLE, YMD_PARTITION, WIKI),
            '--filtering-limit', FILTERING_LIMIT,
        ]
    )

    complete = DummyOperator(task_id='complete')

    wait_for_data >> map_subgraphs_queries >> complete