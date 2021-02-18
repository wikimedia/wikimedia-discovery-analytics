"""Ship mediawiki recommendation create events to elasticsearch

Ingests mediawiki/recommendation/create events from hive tables and writes the
set of pages that need their "recommendation exists" flag enabled to a staging
table to be picked up by the transfer_to_es dag.
"""
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor

from wmf_airflow import DAG
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import \
    MEDIAWIKI_ACTIVE_DC, REPO_PATH, YMDH_PARTITION, DagConf


dag_conf = DagConf('mediawiki_revision_recommendation_create_hourly_conf')

INPUT_TABLE = dag_conf('table_event_recommendation')
OUTPUT_TABLE = dag_conf('table_discovery_recommendation')


with DAG(
    'mediawiki_revision_recommendation_create_init',
    default_args={
        'start_date': datetime(2020, 1, 8),
    },
    schedule_interval='@once',
    user_defined_macros={
        'dag_conf': dag_conf.macro,
    },
) as dag_init:
    complete = DummyOperator(task_id='complete')
    HiveOperator(
        task_id='create_tables',
        hql="""
            CREATE TABLE IF NOT EXISTS {{ dag_conf.table_discovery_recommendation }} (
                `wikiid` string COMMENT 'MediaWiki database name',
                `page_id` int COMMENT 'MediaWiki page id',
                `page_namespace` int COMMENT 'MediaWiki namespace page_id belongs to',
                `recommendation_type` string COMMENT 'The type of the recommendation to create, typically refers to the use case'
            )
            PARTITIONED BY (
                `year` int COMMENT 'Unpadded year data collection starts at',
                `month` int COMMENT 'Unpadded month data collection starts at',
                `day` int COMMENT 'Unpadded day data collection starts at',
                `hour` int COMMENT 'Unpadded hour data collection starts at'
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_path_discovery_recommendation }}'
        """  # noqa
    ) >> complete


with DAG(
    'mediawiki_revision_recommendation_create_hourly',
    default_args={
        'start_date': datetime(2020, 1, 8),
    },
    schedule_interval='@hourly',
    # Allow a second job to start even if the previous is still
    # processing retrys
    max_active_runs=2,
    catchup=True,
) as dag:
    wait_for_data = NamedHivePartitionSensor(
        task_id='wait_for_data',
        # We send a failure email every 6 hours and keep trying for a full day.
        timeout=60 * 60 * 6,
        retries=4,
        email_on_retry=True,
        # Select single hourly partition
        partition_names=[
            '{}/datacenter={}/{}'.format(
                INPUT_TABLE, MEDIAWIKI_ACTIVE_DC, YMDH_PARTITION)
        ])

    # Extract the data from mediawiki event logs and put into
    # a format suitable for shipping to elasticsearch.
    extract_predictions = SparkSubmitOperator(
        task_id='extract_recommendations',
        conf={
            # Delegate retrys to airflow
            'spark.yarn.maxAppAttempts': '1',
            'spark.dynamicAllocation.maxExecutors': '20',
        },
        spark_submit_env_vars={
            'PYSPARK_PYTHON': 'python3.7',
        },
        py_files=REPO_PATH + '/spark/wmf_spark.py',
        application=REPO_PATH + '/spark/prepare_recommendation_create.py',
        application_args=[
            '--input-partition', INPUT_TABLE + '/' + YMDH_PARTITION,
            '--output-partition', OUTPUT_TABLE + '/' + YMDH_PARTITION,
        ],
    )

    complete = DummyOperator(task_id='complete')

    wait_for_data >> extract_predictions >> complete
