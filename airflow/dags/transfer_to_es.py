from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import jinja2
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.swift_upload import SwiftUploadOperator
from wmf_airflow.template import REPO_PATH, DagConf


dag_conf = DagConf('transfer_to_es_conf')

PATH_OUT_WEEKLY = dag_conf('base_output_path') + \
    '/date={{ ds_nodash }}/freq=weekly'
PATH_OUT_HOURLY = dag_conf('base_output_path') + \
    '/date={{ ds_nodash }}/freq=hourly/hour={{ execution_date.hour }}'

# Default kwargs for all Operators
default_args = {
    'owner': 'discovery-analytics',
    # This DAG updates the state of the search engine. The sequence of updates is
    # important for the final state to be correct. As such the previous run must
    # always complete before the next.
    'depends_on_past': True,
    'start_date': datetime(2021, 1, 24),
    'email': ['discovery-alerts@lists.wikimedia.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}


def convert(config_name: str, output_path: str) -> BaseOperator:
    return SparkSubmitOperator(
        task_id='convert_to_esbulk',
        conf={
            'spark.yarn.maxAppAttempts': '1',
            'spark.dynamicAllocation.maxExecutors': '25',
        },
        spark_submit_env_vars={
            'PYSPARK_PYTHON': 'python3.7',
        },
        py_files=REPO_PATH + '/spark/wmf_spark.py',
        application=REPO_PATH + '/spark/convert_to_esbulk.py',
        application_args=[
            '--config', config_name,
            '--namespace-map-table', dag_conf('table_namespace_map'),
            '--output', output_path,
            '--datetime', '{{ execution_date }}'
        ])


def upload(path: str) -> SwiftUploadOperator:
    # Ship to production
    return SwiftUploadOperator(
        task_id='upload_to_swift',
        swift_container=dag_conf('swift_container'),
        source_directory=path,
        swift_object_prefix='{{ ds_nodash }}',
        swift_overwrite=True,
        event_per_object=True)


with DAG(
    'transfer_to_es_hourly',
    default_args=default_args,
    # Five minutes past the hour every hour
    schedule_interval='@hourly',
    max_active_runs=2,
    catchup=True,
    template_undefined=jinja2.StrictUndefined,
) as hourly_dag:
    sensor_kwargs = dict(
        timeout=timedelta(hours=3).total_seconds(),
        retries=4,
        email_on_retry=True)

    # Similarly wait for ores prediction prepartition to run
    ores_articletopic = ExternalTaskSensor(
        task_id='wait_for_ores_predictions',
        external_dag_id='ores_predictions_hourly',
        external_task_id='complete',
        **sensor_kwargs)

    # Format inputs as elasticsearch bulk updates
    convert_to_esbulk = convert('hourly', PATH_OUT_HOURLY)
    upload_to_swift = upload(PATH_OUT_HOURLY)
    complete = DummyOperator(task_id='complete')

    ores_articletopic >> convert_to_esbulk >> upload_to_swift >> complete


with DAG(
    'transfer_to_es_weekly',
    default_args=default_args,
    # Once a week at midnight on Sunday morning
    schedule_interval='0 0 * * 0',
    max_active_runs=1,
    catchup=False,
    template_undefined=jinja2.StrictUndefined,
) as weekly_dag:
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

    convert_to_esbulk = convert('weekly', PATH_OUT_WEEKLY)
    upload_to_swift = upload(PATH_OUT_WEEKLY)
    complete = DummyOperator(task_id='complete')

    popularity_score >> convert_to_esbulk >> upload_to_swift >> complete
