"""Generate daily head queries report for all wikis"""
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator

from wmf_airflow import DAG
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import REPO_PATH, DagConf


dag_conf = DagConf('fulltext_head_queries_conf')


with DAG(
    'fulltext_head_queries_daily',
    default_args={
        # No schedule, but airflow still requires start_date to be a valid datetime
        'start_date': datetime(2021, 1, 1),
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
        # This task reads the fully history of searchsatisfaction,
        # hundreds of GB and thousands of partitions. Keep a cap
        # on how much parallelism spark will try to use.
        max_executors=200,
        application=REPO_PATH + '/spark/fulltext_head_queries.py',
        application_args=[
            '--search-satisfaction-partition', dag_conf('table_search_satisfaction') + '/',
            '--num-queries', dag_conf('num_queries'),
            '--output-partition', output_partition_spec,
        ])

    complete = DummyOperator(task_id='complete')
    head_queries >> complete
