"""Generate daily head queries report for all wikis"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from wmf_airflow import DAG
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import REPO_PATH, DagConf


dag_conf = DagConf('fulltext_head_queries_conf')


with DAG(
    'fulltext_head_queries_daily',
    default_args={
        # No schedule, but airflow still requires start_date to be a valid datetime
        'start_date': days_ago(2),
    },
    # min hour day month dow
    schedule_interval='38 0 * * *',
    max_active_runs=1,
    # single report covers all data, backfill/catchup wouldn't make sense
    catchup=False,
) as dag:
    output_partition_spec = dag_conf('output_table') + '/date={{ ds_nodash }}'

    head_queries = SparkSubmitOperator(
        task_id='head_queries',
        name='airflow: fulltext_head_queries - {{ ds }}',
        conf={
            # Delegate retrys to airflow
            'spark.yarn.maxAppAttempts': '1',
            # This task reads the fully history of searchsatisfaction,
            # hundreds of GB and thousands of partitions. Keep a cap
            # on how much parallelism spark will try to use.
            'spark.dynamicAllocation.maxExecutors': 200,
        },
        spark_submit_env_vars={
            'PYSPARK_PYTHON': 'python3.7',
        },
        py_files=REPO_PATH + '/spark/wmf_spark.py',
        application=REPO_PATH + '/spark/fulltext_head_queries.py',
        application_args=[
            '--search-satisfaction-partition', dag_conf('table_search_satisfaction') + '/',
            '--num-queries', dag_conf('num_queries'),
            '--output-partition', output_partition_spec,
        ])

    complete = DummyOperator(task_id='complete')
    head_queries >> complete
