"""Init DAG
    Create table and partitions for process_sparql_query_hourly dag
   Process Query
    Runs hourly. Ingests sparql queries from input hive table and
    extracts various components of the query. Saves the extracted
    queries in output hive table.

"""
from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor

from wmf_airflow import DAG
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import (
    REPO_PATH, YMDH_PARTITION, DagConf, wmf_conf, eventgate_partitions)


dag_conf = DagConf('process_sparql_query_conf')

ARTIFACTS_DIR = REPO_PATH + "/artifacts"
WDQS_SPARK_TOOLS = ARTIFACTS_DIR + '/rdf-spark-tools-latest-jar-with-dependencies.jar'

INPUT_TABLE = dag_conf('event_sparql_query_table')
OUTPUT_TABLE = dag_conf('discovery_processed_sparql_query_table')
WIKI = dag_conf('wiki')
DATACENTERS = ["eqiad", "codfw"]

HQL = """
CREATE TABLE IF NOT EXISTS {{ dag_conf.discovery_processed_sparql_query_table }} (
    `id`                string  COMMENT 'Query Id',
    `query`             string  COMMENT 'The sparql query',
    `query_time`        bigint  COMMENT 'Time taken to run the query',
    `query_time_class`  string  COMMENT 'Bucketed time taken to run the query',
    `ua`                string  COMMENT 'User agent',
    `q_info`            struct<
                            queryReprinted: string,
                            opList: array<string>,
                            operators: map<string, bigint>,
                            prefixes: map<string, bigint>,
                            nodes: map<string, bigint>,
                            services: map<string, bigint>,
                            wikidataNames: map<string, bigint>,
                            expressions: map<string, bigint>,
                            paths: map<string, bigint>,
                            triples: array<
                                struct<
                                    subjectNode: struct<nodeType: string,nodeValue: string>,
                                    predicateNode: struct<nodeType: string,nodeValue: string>,
                                    objectNode: struct<nodeType: string,nodeValue: string>
                                    >
                                >
                            >
                                COMMENT 'Extracted information from the query string'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of queries',
    `month`             int     COMMENT 'Unpadded month of queries',
    `day`               int     COMMENT 'Unpadded day of queries',
    `hour`              int     COMMENT 'Unpadded hour of queries',
    `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
)
STORED AS PARQUET
LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_processed_sparql_query_location }}'
"""

# Default kwargs for all Operators
default_args = {
    'start_date': datetime(2021, 6, 1),
}

with DAG(
    'process_sparql_query_init',
    default_args=default_args,
    schedule_interval='@once',
    user_defined_macros={
        'dag_conf': dag_conf.macro,
        'wmf_conf': wmf_conf.macro,
    },
) as dag_init:
    complete = DummyOperator(task_id='complete')
    HiveOperator(
        task_id='create_tables',
        hql=HQL
    ) >> complete


with DAG(
    'process_sparql_query_hourly',
    default_args=default_args,
    schedule_interval='@hourly',
    # The spark job uses minimal internal parallelism, increase top level
    # parallelism to allow backfills to complete in under a week.
    max_active_runs=8,
    catchup=True,
) as dag:

    # Select single hourly partition
    output_table_and_partition = '%s/%s/wiki=%s' % (OUTPUT_TABLE, YMDH_PARTITION, WIKI)
    input_table_and_partition = '%s/%s' % (INPUT_TABLE, YMDH_PARTITION)

    wait_for_data = NamedHivePartitionSensor(
        task_id='wait_for_data',
        # to be changed to hours=6 after backfill completes
        sla=timedelta(days=365),
        retries=4,
        partition_names=eventgate_partitions(INPUT_TABLE),
    )

    # Extract the sparql queries from table and
    # process query to proper format for saving.
    extract_queries = SparkSubmitOperator(
        task_id='extract_queries',
        conf={
            # Delegate retries to airflow
            'spark.yarn.maxAppAttempts': '1',
        },
        application=WDQS_SPARK_TOOLS,
        java_class="org.wikidata.query.rdf.spark.analysis.QueryExtractor",
        # The job is a straight map, no shuffle occurs and the data is
        # small enough (< 200MB per run) that the job doesn't use more than
        # a couple tasks, which fit in a single executor.
        max_executors=1,
        executor_cores=8,
        executor_memory="16g",
        driver_memory="2g",
        application_args=[
            '--input-table', input_table_and_partition,
            '--output-table', output_table_and_partition,
        ]
    )

    complete = DummyOperator(task_id='complete')

    wait_for_data >> extract_queries >> complete
