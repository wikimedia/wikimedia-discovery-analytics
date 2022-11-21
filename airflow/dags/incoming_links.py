from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import jinja2
from wmf_airflow import DAG
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import REPO_PATH, DagConf, wmf_conf

dag_conf = DagConf('incoming_links_conf')
default_args = {
    # first execution week of 11/13 - 11/20
    'start_date': datetime(2022, 11, 13),
}

with DAG(
    'incoming_links_init',
    schedule_interval='@once',
    default_args=default_args,
    user_defined_macros={
        'dag_conf': dag_conf.macro,
        'wmf_conf': wmf_conf.macro,
    },
    template_undefined=jinja2.StrictUndefined
) as dag_init:
    HiveOperator(
        task_id='create_tables',
        hql="""
            CREATE TABLE {{ dag_conf.incoming_links_table }} (
              `wikiid` string,
              `page_id` bigint,
              `page_namespace` int,
              `dbkey` string,
              `incoming_links` bigint)
            PARTITIONED BY (
              `snapshot` string)
            STORED AS parquet
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.incoming_links_location }}'
        """
    ) >> DummyOperator(task_id='complete')


with DAG(
    'incoming_links_weekly',
    default_args=default_args,
    user_defined_macros={
        'dag_conf': dag_conf.macro,
    },
    schedule_interval='0 0 * * 0',
    max_active_runs=1,
    catchup=True,
    template_undefined=jinja2.StrictUndefined,
) as weekly_dag:
    # Waiting for specific hive data is difficult, as the source
    # job writes many partitions. Instead wait for the complete task
    wait_for_data = ExternalTaskSensor(
        task_id='wait_for_data',
        mode='reschedule',
        sla=timedelta(days=1),
        external_dag_id='import_cirrus_indexes_weekly',
        external_task_id='complete',
    )

    generate = SparkSubmitOperator(
        task_id='generate_incoming_link_counts',
        conf={
            # Reading the dumps requires a little extra memory, some rows are giant.
            'spark.executor.memoryOverhead': '768M',
            # spark spends a lot of time running only a couple tasks. It might have 400 cores
            # in active executors, but only run 4 active tasks. This seems to fix that
            'spark.locality.wait': '0',
            # The incoming links aggregation emits 11 billion rows, this seems
            # to avoid shuffle fetch failures/task retries when dealing with that.
            'spark.reducer.maxReqsInFlight': '1',
            'spark.shuffle.io.retryWait': '120s',
        },
        max_executors=200,
        # Due to SPARK-20880 reading avro from spark2 must be done with a single executor per jvm
        executor_cores=1,
        # The total job involves ~75k tasks, a little extra memory seems to help the driver
        # avoid large pauses.
        driver_memory='4g',
        application=REPO_PATH + '/spark/incoming_links.py',
        application_args=[
            '--cirrus-dump', '{{ dag_conf.cirrus_indexes_table }}/snapshot={{ ds_nodash }}',
            '--output-partition', '{{ dag_conf.incoming_links_table }}/snapshot={{ ds_nodash }}',
        ]
    )

    wait_for_data >> generate >> DummyOperator(task_id='complete')
