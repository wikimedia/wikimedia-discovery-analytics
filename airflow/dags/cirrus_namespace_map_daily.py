"""Maintain a mapping from wikiid + namespace_id to elasticsearch index"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

import jinja2
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import HTTPS_PROXY, REPO_PATH, DagConf


dag_conf = DagConf('cirrus_namespace_map_daily_conf')

default_args = {
    'owner': 'discovery-analytics',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 25),
    'email': ['discovery-alerts@lists.wikimedia.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

with DAG(
    'cirrus_namespace_map_daily',
    default_args=default_args,
    # expression order: min hour month dom dow
    schedule_interval='0 1 * * *',
    # The dag overwrites, catchup would only add repeated work
    catchup=False,
    # The dag overwrites data, so running two at a time would be silly
    max_active_runs=1,
    # no undefined variables allowed in templates
    template_undefined=jinja2.StrictUndefined,
) as dag:
    fetch_namespace_map = SparkSubmitOperator(
        task_id='fetch_namespace_map',
        conf={
            'spark.yarn.maxAppAttempts': 1,
            # Could be 1, spark is only really being used for
            # hive/hdfs/parquet integration used by downstream consumers
            'spark.dynamicAllocation.maxExecutors': 10,
            # Provide access to public internet to query prod apis
            'spark.executorEnv.https_proxy': HTTPS_PROXY,
        },
        spark_submit_env_vars={
            'PYSPARK_PYTHON': 'python3.7',
        },
        py_files=REPO_PATH + '/spark/wmf_spark.py',
        application=REPO_PATH + '/spark/fetch_cirrussearch_namespace_map.py',
        application_args=[
            # The trailing '/' indicates they are unpartitioned tables.
            # Current convention is that config specifys tables, DAGS specify partitions.
            '--canonical-wikis-partition', dag_conf('table_canonical_wikis') + '/',
            '--output-partition', dag_conf('table_output') + '/',
        ]
    )

    complete = DummyOperator(task_id='complete')

    fetch_namespace_map >> complete
