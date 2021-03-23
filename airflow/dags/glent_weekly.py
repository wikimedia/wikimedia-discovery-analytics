"""Run the glent query suggestions pipeline"""

from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from wmf_airflow import DAG
from wmf_airflow.hdfs_cli import HdfsCliHook
from wmf_airflow.hive_partition_range_sensor import HivePartitionRangeSensor
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.swift_upload import SwiftUploadOperator
from wmf_airflow.template import REPO_PATH, DagConf, eventgate_partition_range


dag_conf = DagConf('glent_conf')


def dag_conf_days_as_sec(key):
    return '{{ macros.timedelta(days=var.json.glent_conf.%s).total_seconds() }}' % (key)


TABLE_CANONICAL_WIKIS = dag_conf('table_canonical_wikis')
TABLE_CIRRUS_EVENT = dag_conf('table_cirrus_event')
TABLE_M0_PREP = dag_conf('table_m0_prep')
TABLE_M1_PREP = dag_conf('table_m1_prep')
TABLE_SUGGESTIONS = dag_conf('table_suggestions')

GLENT_JAR_PATH = REPO_PATH + '/artifacts/glent-latest-jar-with-dependencies.jar'

# Temporary paths for each run, deleted after dagrun is done (success or fail)
TEMP_DIR = dag_conf('base_temp_dir') + '/{{ dag.dag_id }}_{{ ds }}'
TEMP_CANDIDATES_DIR = TEMP_DIR + '/query_similarity_candidates'
TEMP_ESBULK_DIR = TEMP_DIR + '/esbulk'

# Glent jobs were converted from oozie, which dates jobs with the end
# of a period, to airflow that dates jobs with the start. For this reason
# all of the outputs continue the way oozie dated partitions, and the
# "current" output is written to what airflow considers date + 7 days.

# timestamps used to select ranges in glent
PREV_INSTANCE = '{{ ds }}T00:00:00Z'
CUR_INSTANCE = '{{ macros.ds_add(ds, 7) }}T00:00:00Z'
LEGAL_CUTOFF = '{{ macros.ds_add(ds, -77) }}T00:00:00Z'

# partition names
PREV_PARTITION = '{{ ds_nodash }}'
CUR_PARTITION = '{{ macros.ds_format(macros.ds_add(ds, 7), "%Y-%m-%d", "%Y%m%d") }}'


def glent_op(
    task_id, conf={}, executor_memory='6G',
    executor_cores='3', driver_memory='1G', **kwargs,
):
    """Helper applies defaults for invoking glent via spark"""
    return SparkSubmitOperator(
        task_id=task_id,
        conf=dict({
            'spark.yarn.maxAppAttempts': 1,
            'spark.sql.shuffle.partitions': 200,
            'spark.dynamicAllocation.maxExecutors': 70,
            'spark.sql.executor.memoryOverhead': '640M',
            'spark.sql.shuffle.service.enabled': 'true',
        }, **conf),
        executor_memory=executor_memory,
        executor_cores=executor_cores,
        driver_memory=driver_memory,
        application=GLENT_JAR_PATH,
        **kwargs
    )


with DAG(
    'glent_weekly',
    default_args={
        'start_date': datetime(2020, 4, 25),
    },
    # Once a week at 2am saturday morning. This lines up with the prior
    # oozie job schedule.
    # expression order: min hour month dom dow
    schedule_interval='0 2 * * 6',
    # As a weekly job there should never be more than one running at a time.
    max_active_runs=1,
    catchup=True,
) as dag:
    # Wait for backend logs from CirrusSearch
    wait_for_data = HivePartitionRangeSensor(
        task_id='wait_for_data',
        timeout=int(timedelta(days=1).total_seconds()),
        email_on_retry=True,
        table=TABLE_CIRRUS_EVENT,
        period=timedelta(days=7),
        partition_frequency='hours',
        partition_specs=eventgate_partition_range())

    # Merge the recent week of events into m0prep
    merge_session_similarity = glent_op(
        task_id='merge_session_similarity',
        # We maintain a single partition of data that has new data merged in
        # and old data dropped each week. For this to work a merge can only run
        # if the previous merge was a success.
        depends_on_past=True,
        application_args=[
            'm0prep',
            # WithCirrusLog
            '--wmf-log-name', TABLE_CIRRUS_EVENT,
            '--log-ts-from', PREV_INSTANCE,
            '--log-ts-to', CUR_INSTANCE,
            '--max-n-queries-per-ident', dag_conf('max_n_queries_per_ident'),
            '--map-wikiid-to-lang-name', TABLE_CANONICAL_WIKIS,
            # WithGlentPartition
            '--input-table', TABLE_M0_PREP,
            '--input-partition', PREV_PARTITION,
            # WithSuggestionFilter
            '--max-edit-dist-sugg', dag_conf('max_edit_dist_sugg'),
            '--max-norm-edit-dist', dag_conf('max_norm_edit_dist'),
            '--min-hits-diff', dag_conf('min_hits_diff'),
            '--min-hits-perc-diff', dag_conf('min_hits_perc_diff'),
            # WithPrepOutput
            '--output-table', TABLE_M0_PREP,
            '--output-partition', CUR_PARTITION,
            '--max-output-partitions', '5',
            # use custom normalizer
            '--query-normalizer', dag_conf('query_normalizer_m0')
        ]
    )

    # Merge the recent week of events into m1prep
    merge_query_similarity = glent_op(
        task_id='merge_query_similarity',
        # We maintain a single partition of data that has new data merged in
        # and old data dropped each week. For this to work a merge can only run
        # if the previous merge was a success.
        depends_on_past=True,
        application_args=[
            'm1prep',
            '--wmf-log-name', TABLE_CIRRUS_EVENT,
            '--log-ts-from', PREV_INSTANCE,
            '--log-ts-to', CUR_INSTANCE,
            '--max-n-queries-per-ident', dag_conf('max_n_queries_per_ident'),
            '--map-wikiid-to-lang-name', TABLE_CANONICAL_WIKIS,
            '--input-table', TABLE_M1_PREP,
            '--input-partition', PREV_PARTITION,
            '--earliest-legal-ts', LEGAL_CUTOFF,

            '--output-table', TABLE_M1_PREP,
            '--output-partition', CUR_PARTITION,
            # use custom normalizer
            '--query-normalizer', dag_conf('query_normalizer_m1'),
        ]
    )

    generate_dictionary_similarity_suggestions = glent_op(
        task_id='generate_dictionary_similarity_suggestions',
        # We maintain a single partition of data that has new data merged in
        # and old data dropped each week. For this to work a merge can only run
        # if the previous merge was a success.
        depends_on_past=True,
        application_args=[
            'm2run',
            '--wmf-log-name', TABLE_CIRRUS_EVENT,
            '--log-ts-from', PREV_INSTANCE,
            '--log-ts-to', CUR_INSTANCE,
            '--max-n-queries-per-ident', dag_conf('max_n_queries_per_ident'),
            '--map-wikiid-to-lang-name', TABLE_CANONICAL_WIKIS,

            '--input-table', TABLE_SUGGESTIONS,
            '--input-partition', PREV_PARTITION,
            '--earliest-legal-ts', LEGAL_CUTOFF,

            '--output-table', TABLE_SUGGESTIONS,
            '--output-partition', CUR_PARTITION,
            '--max-output-partitions', '2'
        ])

    # Suggestions generation, particularly for query similarity, is an
    # expensive process. When backfilling only the most recent suggestions
    # will be used, so don't bother generating the old suggestions.
    latest_only = LatestOnlyOperator(task_id='latest_only')

    # Generate and filter session similarity suggestions.
    generate_session_similarity_suggestions = glent_op(
        task_id='generate_session_similarity_suggestions',
        application_args=[
            'm0run',
            '--input-table', TABLE_M0_PREP,
            '--input-partition', CUR_PARTITION,
            '--output-table', TABLE_SUGGESTIONS,
            '--output-partition', CUR_PARTITION,
            '--max-output-partitions', '5',
        ],
    )

    # Generate potential query similarity suggestion candidates. Separated from
    # candidate filtering to allow different cpu/memory ratio for a heavy and
    # long running job.
    generate_query_similarity_candidates = glent_op(
        task_id='generate_query_similarity_candidates',
        # This is a very heavy job, put it in the sequential queue that
        # prevents multiple heavy jobs from running concurrently.
        pool='sequential',
        conf={
            # Increase required from defaults for returning
            # the FSTs to the driver
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
        application_args=[
            'm1run.candidates',
            '--input-table', TABLE_M1_PREP,
            '--input-partition', CUR_PARTITION,
            '--output-directory', TEMP_CANDIDATES_DIR,
            '--num-fst', '1',
        ],
    )

    resolve_query_similarity_suggestions = glent_op(
        task_id='resolve_query_similary_suggestions',
        # This is a very heavy job, put it in the sequential queue that
        # prevents multiple heavy jobs from running concurrently.
        pool='sequential',
        conf={
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
        application_args=[
            'm1run',
            '--input-table', TABLE_M1_PREP,
            '--input-partition', CUR_PARTITION,
            '--candidates-directory', TEMP_CANDIDATES_DIR,

            '--max-edit-dist-sugg', dag_conf('max_edit_dist_sugg'),
            '--max-norm-edit-dist', dag_conf('max_norm_edit_dist'),
            '--min-hits-diff', dag_conf('min_hits_diff'),
            '--min-hits-perc-diff', dag_conf('min_hits_perc_diff'),

            '--output-table', TABLE_SUGGESTIONS,
            '--output-partition', CUR_PARTITION,
            '--max-output-partitions', '100',
        ])

    esbulk = glent_op(
        task_id='esbulk',
        application_args=[
            'esbulk',
            '--input-table', TABLE_SUGGESTIONS,
            '--input-partition', CUR_PARTITION,
            '--output-path', TEMP_ESBULK_DIR,
            '--version-marker', CUR_PARTITION,
        ])

    swift_upload = SwiftUploadOperator(
        task_id='swift_upload',
        swift_container=dag_conf('swift_container'),
        source_directory=TEMP_ESBULK_DIR,
        swift_object_prefix=CUR_PARTITION,
        swift_overwrite=True,
        swift_auth_file=dag_conf('swift_auth_file'),
        swift_delete_after=dag_conf_days_as_sec('swift_delete_after_days'))

    complete = DummyOperator(task_id='complete')

    cleanup_temp_path = PythonOperator(
        task_id='cleanup_temp_path',
        trigger_rule=TriggerRule.ALL_DONE,
        python_callable=HdfsCliHook.rm,
        op_args=[TEMP_DIR],
        op_kwargs={'recurse': True, 'force': True},
        provide_context=False)

    # Data collection stage: reads from previous run
    # glent partitions and event logs
    (wait_for_data
        >> [
            merge_session_similarity, merge_query_similarity,
            generate_dictionary_similarity_suggestions
        ] >> latest_only)

    # Session similarity suggestions (method 0)
    latest_only >> generate_session_similarity_suggestions >> esbulk

    # Query similarity suggestions (method 1). This is separate from above
    # because it has an additional dependency compared to the simpler
    # suggestion methods.
    (latest_only
        >> generate_query_similarity_candidates
        >> resolve_query_similarity_suggestions
        >> esbulk)

    # Final stage: ship to prod and cleanup.
    esbulk >> swift_upload >> complete >> cleanup_temp_path
