from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.hive_partition_range_sensor_plugin import HivePartitionRangeSensor


INPUT_TABLE = 'event.mediawiki_revision_score'
OUTPUT_TABLE = 'discovery.ores_articletopic'

MODEL = 'articletopic'

# Only export topics with probability >= threshold
PROBABILITY_THRESHOLD = 0.5

# Path to root of this repository (wikimedia/discovery/analytics) on
# the airflow servers
REPO_BASE = '{{ var.value.wikimedia_discovery_analytics_path }}'

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
    'ores_predictions_weekly',
    default_args=default_args,
    # Once a week at midnight on Sunday morning
    schedule_interval='0 0 * * 0',
    # As a weekly job there should never really be more than
    # one running at a time.
    max_active_runs=1,
    catchup=False,
) as dag:
    wait_for_data = HivePartitionRangeSensor(
        task_id='wait_for_data',
        # We send a failure email once a day when the expected data is not
        # found. Since this is a weekly job we wait up to 4 days for the data
        # to show up before giving up and waiting for next scheduled run.
        timeout=60 * 60 * 24,  # 24 hours
        retries=4,
        email_on_retry=True,
        # partition range selection
        table=INPUT_TABLE,
        period=timedelta(days=7),
        partition_frequency='hours',
        partition_specs=[
            # While multiple datacenters exist, only 'eqiad' is being populated
            # as of 2020-1-12. The last codfw parition seems to be 2019-9-9.
            [
                ('datacenter', 'eqiad'), ('year', None),
                ('month', None), ('day', None), ('hour', None)
            ]
        ])

    # Extract the data from mediawiki event logs and put into
    # a format suitable for shipping to elasticsearch.
    extract_predictions = SparkSubmitOperator(
        task_id='extract_predictions',
        conf={
            'spark.pyspark.python': 'python3',
            # Delegate retrys to airflow
            'spark.yarn.maxAppAttempts': '1',
        },
        application=REPO_BASE + '/spark/prepare_mw_rev_score.py',
        application_args=[
            '--input-table', INPUT_TABLE,
            '--output-table', OUTPUT_TABLE,
            '--start-date', '{{ ds }}',
            '--end-date', '{{ macros.ds_add(ds, 7) }}',
            '--threshold', str(PROBABILITY_THRESHOLD),
            '--prediction', MODEL
        ],
    )
    wait_for_data >> extract_predictions

    complete = DummyOperator(task_id='complete')
    extract_predictions >> complete
