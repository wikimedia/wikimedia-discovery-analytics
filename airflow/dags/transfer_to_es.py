from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor

from wmf_airflow import DAG
from wmf_airflow.transfer_to_es import convert_and_upload


# Default kwargs for all Operators
default_args = {
    # This DAG updates the state of the search engine. The sequence of updates is
    # important for the final state to be correct. As such the previous run must
    # always complete before the next.
    'depends_on_past': True,
    'start_date': datetime(2021, 1, 24),
}


sensor_kwargs = dict(
    timeout=timedelta(hours=3).total_seconds(),
    retries=4,
    email_on_retry=True)


with DAG(
    'transfer_to_es_hourly',
    default_args=default_args,
    # Five minutes past the hour every hour
    schedule_interval='@hourly',
    max_active_runs=2,
    catchup=True,
) as hourly_dag:
    sensors = [
        ExternalTaskSensor(
            task_id='wait_for_ores_predictions',
            mode='reschedule',
            external_dag_id='ores_predictions_hourly',
            external_task_id='complete',
            **sensor_kwargs
        ),
        ExternalTaskSensor(
            task_id='wait_for_recommendations',
            mode='reschedule',
            external_dag_id='mediawiki_revision_recommendation_create_hourly',
            external_task_id='complete',
            **sensor_kwargs
        ),
    ]

    convert, upload = convert_and_upload(
        'hourly',
        'freq=hourly/hour={{ execution_date.hour }}',
        'swift.search_updates_prioritized.upload-complete')

    sensors >> convert >> upload >> DummyOperator(task_id='complete')


with DAG(
    'transfer_to_es_weekly',
    default_args=default_args,
    # Once a week at midnight on Sunday morning
    schedule_interval='0 0 * * 0',
    max_active_runs=1,
    catchup=True,
) as weekly_dag:
    # Wait for popularity to compute
    sensors = [
        ExternalTaskSensor(
            task_id='wait_for_popularity_score',
            mode='reschedule',
            external_dag_id='popularity_score_weekly',
            external_task_id='complete',
            **sensor_kwargs
        ),
        NamedHivePartitionSensor(
            task_id='wait_for_image_recommendations',
            mode='reschedule',
            partition_names=[
                "analytics_platform_eng.image_suggestions_search_index_delta/snapshot="
                + "{{ execution_date.add(days=-6).format('%Y-%m-%d') }}",
            ],
            **sensor_kwargs
        ),
    ]

    convert, upload = convert_and_upload('weekly', 'freq=weekly')
    sensors >> convert >> upload >> DummyOperator(task_id='complete')


with DAG(
    'image_suggestions_manual',
    default_args=default_args,
    schedule_interval=None,
) as imagerec_dag:
    convert, upload = convert_and_upload(
        'image_suggestion_manual',
        'freq=manual/image_suggestions')
    convert >> upload >> DummyOperator(task_id='complete')
