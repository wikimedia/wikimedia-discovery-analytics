"""Run the glent query similarity (m1) suggestion pipeline
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from wmf_airflow.hdfs_cli import HdfsCliHook
from wmf_airflow.hive_partition_range_sensor import HivePartitionRangeSensor
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import REPO_BASE


def dag_conf(key):
    """DAG specific configuration stored in airflow variable"""
    return '{{ var.json.glent_similar_queries_conf.%s }}' % key


TABLE_CANONICAL_WIKIS = dag_conf('table_canonical_wikis')
TABLE_CIRRUS_EVENT = dag_conf('table_cirrus_event')
TABLE_M1_PREP = dag_conf('table_m1_prep')
TABLE_SUGGESTIONS = dag_conf('table_suggestions')

GLENT_JAR_PATH = REPO_BASE + '/artifacts/glent-0.1.1-jar-with-dependencies.jar'

NAME_NODE = 'hdfs://analytics-hadoop'
TEMP_CANDIDATES_DIR = dag_conf('base_temp_dir') + '/{{ dag.dag_id }}_{{ ds }}'

default_args = {
    'owner': 'discovery-analytics',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 2),
    'email': ['ebernhardson@wikimedia.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}


with DAG(
    'glent_similar_queries_weekly',
    default_args=default_args,
    # Once a week at 2am saturday morning. This lines up with the glent
    # oozie jobs.
    # expression order: min hour month dom dow
    schedule_interval='0 2 * * 6',
    # As a weekly job there should never be more than one running at a time.
    max_active_runs=1,
    catchup=True,
) as dag:
    # Wait for oozie to complete the m1prep stage
    wait_for_data = HivePartitionRangeSensor(
        task_id='wait_for_data',
        timeout=int(timedelta(days=1).total_seconds()),
        email_on_retry=True,
        table=TABLE_CIRRUS_EVENT,
        period=timedelta(days=7),
        partition_frequency='hours',
        partition_specs=[
            [
                ('datacenter', 'eqiad'), ('year', None),
                ('month', None), ('day', None), ('hour', None)
            ]
        ])

    # This job was converted from oozie, which dates jobs with the end of
    # a period, to airflow that dates jobs with the start. For this reason
    # all of the outputs continue the way oozie dated partitions, and the
    # "current" output is written to what airflow considers date + 7 days.

    # timestamps used to select ranges in glent
    prev_instance = '{{ ds }}T00:00:00Z'
    cur_instance = "{{ macros.ds_add(ds, 7) }}T00:00:00Z"
    legal_cutoff = "{{ macros.ds_add(ds, -77) }}T00:00:00Z"
    # Partition names
    prev_partition = '{{ ds_nodash }}'
    cur_partition = '{{ macros.ds_format(macros.ds_add(ds, 7), "%Y-%m-%d", "%Y%m%d") }}'

    # Merge the recent week of events into m1prep
    merge_events = SparkSubmitOperator(
        task_id='merge_events',
        # We maintain a single partition of data that has new data merged in
        # and old data dropped each week. For this to work a merge can only run
        # if the previous merge was a success.
        depends_on_past=True,
        conf={
            'spark.yarn.maxAppAttempts': '1',
            # While allocation limits can give us up to 600 tasks,
            # we keep partition counts at 200 as multiple tasks
            # execute in parallel.
            'spark.sql.shuffle.partitions': 200,
            'spark.dynamicAllocation.maxExecutors': 200,
            'spark.sql.executor.memoryOverhead': '640M',
        },
        executor_memory='6G',
        executor_cores='3',
        driver_memory='1G',
        application=GLENT_JAR_PATH,
        application_args=[
            'm1prep',
            '--wmf-log-name', TABLE_CIRRUS_EVENT,
            '--log-ts-from', prev_instance,
            '--log-ts-to', cur_instance,
            '--max-n-queries-per-ident', '2000',
            '--map-wikiid-to-lang-name', TABLE_CANONICAL_WIKIS,
            '--input-table', TABLE_M1_PREP,
            '--input-partition', prev_partition,
            '--earliest-legal-ts', legal_cutoff,

            '--output-table', TABLE_M1_PREP,
            '--output-partition', cur_partition,
        ]
    )

    # Generate potential suggestion candidates. Separated from
    # m1run to allow different cpu/memory ratio for a heavy
    # and long running job.
    generate_candidates = SparkSubmitOperator(
        task_id='generate_candidates',
        conf={
            'spark.yarn.maxAppAttempts': '1',
            # Increase required from defaults for returning
            # the FST to the driver
            'spark.driver.maxResultSize': '4096M',
            # This allocates ~900GB and 800 cores, almost
            # half the available cores.
            'spark.dynamicAllocation.maxExecutors': 50,
        },
        # FST evaluation has minimal memory requirements, and
        # sharing a large data structure between all tasks on
        # same executor. Size up executors accordingly.
        executor_memory='16G',
        executor_cores='16',
        # Final FST merge occurs on the driver. 24G OOMs as of may 2020.
        driver_memory='32G',
        application=GLENT_JAR_PATH,
        application_args=[
            'm1run.candidates',
            '--input-table', TABLE_M1_PREP,
            '--input-partition', cur_partition,
            '--output-directory', TEMP_CANDIDATES_DIR,
            '--num-fst', '1',
        ],
    )

    resolve_suggestions = SparkSubmitOperator(
        task_id='m1run_filter_candidates',
        conf={
            'spark.yarn.maxAppAttempts': '1',
            'spark.sql.shuffle.service.enabled': 'true',
            'spark.sql.shuffle.partitions': 2500,
            'spark.dynamicAllocation.maxExecutors': 150,
            # The joins performed in here generation billions
            # of rows. To avoid `FetchFailedException` on the largest
            # joins we need a few extra parameters.
            # https://stackoverflow.com/a/49781377
            'spark.reducer.maxReqsInFlight': 1,
            'spark.shuffle.io.retryWait': '120s',
            'spark.shuffle.io.maxRetries': 10,
        },
        executor_memory='8G',
        executor_cores='4',
        application=GLENT_JAR_PATH,
        application_args=[
            'm1run',
            '--input-table', TABLE_M1_PREP,
            '--input-partition', cur_partition,
            '--candidates-directory', TEMP_CANDIDATES_DIR,

            '--max-leven-dist-sugg', '3',
            '--max-norm-leven-dist', '1.5',
            '--min-hits-diff', '100',
            '--min-hits-perc-diff', '10',

            '--output-table', TABLE_SUGGESTIONS,
            '--output-partition', cur_partition,
            '--max-output-partitions', '100',
        ])

    cleanup_temp_path = PythonOperator(
        task_id='cleanup_temp_path',
        trigger_rule=TriggerRule.ALL_DONE,
        python_callable=HdfsCliHook.rm,
        op_args=[TEMP_CANDIDATES_DIR],
        op_kwargs={'recurse': True, 'force': True},
        provide_context=False)

    complete = DummyOperator(task_id='complete')

    wait_for_data >> merge_events \
        >> generate_candidates >> resolve_suggestions \
        >> cleanup_temp_path >> complete

    # cleanup_temp_path runs on success or failure, link resolve_suggestions to
    # complete to link together their success.
    resolve_suggestions >> complete
