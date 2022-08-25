"""Coalesce query events and feed them into relforge"""
from datetime import datetime, timedelta
from wmf_airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import YMDH_PARTITION, REPO_PATH, DagConf, eventgate_partitions
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor

dag_conf = DagConf('export_queries_to_relforge_conf')

# Default kwargs for all Operators
default_args = {
    'start_date': datetime(2021, 2, 18)
}

SEARCH_SATISFACTION_TABLE = dag_conf('table_search_satisfaction')
CIRRUSSEARCH_REQUEST_TABLE = dag_conf('mediawiki_cirrussearch_request')


def get_wait_sensor(table: str, sensor_name: str) -> NamedHivePartitionSensor:
    return NamedHivePartitionSensor(
        task_id='wait_for_data_in_{}'.format(sensor_name),
        mode='reschedule',
        # We send a failure email every 6 hours and keep trying for a full day.
        timeout=60 * 60 * 6,
        retries=4,
        # temporary sla change to accomodate the catchup, should be reverted afterwards
        sla=timedelta(days=365),
        # Select single hourly partition
        partition_names=eventgate_partitions(table))


with DAG(
        'export_queries_to_relforge',
        default_args=default_args,
        # min hour day month dow
        schedule_interval='38 * * * *',
        max_active_runs=1,
        catchup=True
) as dag:
    export_queries_to_relforge = SparkSubmitOperator(
        task_id='export_queries_to_relforge',
        max_executors=10,
        jars=REPO_PATH + '/artifacts/elasticsearch-hadoop-7.10.2.jar',
        files=REPO_PATH + '/spark/resources/queries_index_settings.json',
        application=REPO_PATH + '/spark/export_queries_to_relforge.py',
        application_args=[
            '--search-satisfaction-partition', SEARCH_SATISFACTION_TABLE + '/' + YMDH_PARTITION,
            '--cirrus-events-partition', CIRRUSSEARCH_REQUEST_TABLE + '/' + YMDH_PARTITION,
            '--elastic-host', dag_conf('elastic_host'),
            '--elastic-port', dag_conf('elastic_port'),
            '--elastic-index', dag_conf('elastic_index'),
            '--elastic-template', dag_conf('elastic_template')
        ]
    )

    wait_for_search_satisfaction_data = get_wait_sensor(SEARCH_SATISFACTION_TABLE,
                                                        'search_satisfaction')
    wait_for_cirrussearch_data = get_wait_sensor(CIRRUSSEARCH_REQUEST_TABLE,
                                                 'cirrussearch_request')
    complete = DummyOperator(task_id='complete')

    [wait_for_search_satisfaction_data, wait_for_cirrussearch_data] \
        >> export_queries_to_relforge >> complete
