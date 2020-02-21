from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.swift_upload_plugin import SwiftUploadOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor


APPLICATION = '{{ var.value.wikimedia_discovery_analytics_path }}/spark/convert_to_esbulk.py'
PATH_OUT = 'hdfs://analytics-hadoop/wmf/data/discovery/transfer_to_es/date={{ ds_nodash }}'

# Default kwargs for all Operators
default_args = {
    'owner': 'discovery-analytics',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 8),
    'email': ['discovery-alerts@lists.wikimedia.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}


with DAG(
    'transfer_to_es_weekly',
    default_args=default_args,
    # Once a week at midnight on Sunday morning
    schedule_interval='0 0 * * 0',
    # This DAG updates the state of the search engine.  We must never update
    # it with older data than we have already shipped. The previous run must
    # always complete (success or failure) before the next.
    max_active_runs=1,
    catchup=False,
) as dag:
    # Wait for popularity to compute
    popularity_score = ExternalTaskSensor(
        task_id='wait_for_popularity_score',
        # We send a failure email once a day when the expected
        # data is not found. Since this is a weekly job we
        # wait up to 4 days for the data to show up before
        # giving up and waiting for next scheduled run.
        timeout=60 * 60 * 24,  # 24 hours
        retries=4,
        email_on_retry=True,
        # external task selection
        external_dag_id='popularity_score_weekly',
        external_task_id='complete')

    ores_articletopic = ExternalTaskSensor(
        task_id='wait_for_ores_predictions',
        # Same sensor reasoning and config as above
        timeout=60 * 60 * 24,  # 24 hours
        retries=4,
        email_on_retry=True,
        # external task selection
        external_dag_id='ores_predictions_weekly',
        external_task_id='complete')

    # Format inputs as elasticsearch bulk updates
    convert_to_esbulk = SparkSubmitOperator(
        task_id='convert_to_esbulk',
        conf={
            'spark.pyspark.python': 'python3',
            # Delegate retrys to airflow
            'spark.yarn.maxAppAttempts': '1',
        },
        application=APPLICATION,
        application_args=[
            '--output', PATH_OUT,
            '--date', '{{ ds }}',
        ])
    [popularity_score, ores_articletopic] >> convert_to_esbulk

    # Ship to production
    swift_upload = SwiftUploadOperator(
        task_id='upload_to_swift',
        swift_container='search_popularity_score',
        source_directory=PATH_OUT,
        swift_object_prefix='{{ ds_nodash }}',
        swift_overwrite=True,
        event_per_object=True)
    convert_to_esbulk >> swift_upload

    complete = DummyOperator(task_id='complete')
    swift_upload >> complete