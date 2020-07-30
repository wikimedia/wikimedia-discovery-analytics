from datetime import datetime, timedelta
import shlex
from typing import Optional

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from wmf_airflow.template import REPO_PATH, ANALYTICS_REFINERY_PATH


def refinery_drop_hive_partitions(
    database: str,
    older_than_days: int = 60,
    tables: Optional[str] = None,
    partition_depth: Optional[int] = None,
    *args, **kwargs
):
    # Refinery isn't packaged like our own python, so it's hard
    # to invoke remotely with skein. Simply execute it locally
    # with a venv that has the appropriate dependencies.
    executable = '/usr/bin/env'
    arguments = [
        'PYTHONPATH={}/python/'.format(ANALYTICS_REFINERY_PATH),
        REPO_PATH + '/environments/refinery/venv/bin/python',
        ANALYTICS_REFINERY_PATH + '/bin/refinery-drop-hive-partitions',
        '--verbose',
        '--database=' + database,
        '--older-than-days=' + str(older_than_days),
    ]
    if tables is not None:
        arguments.append('--tables=' + tables)
    if partition_depth is not None:
        arguments.append('--partition-depth=' + str(partition_depth))

    safe_arguments = ' '.join(shlex.quote(x) for x in arguments)
    bash_command = executable + ' ' + safe_arguments

    return BashOperator(bash_command=bash_command, *args, **kwargs)


# Default kwargs for all Operators
default_args = {
    'owner': 'discovery-analytics',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 22),
    'email': ['discovery-alerts@lists.wikimedia.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

with DAG(
    'drop_old_data_daily',
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=2,
    catchup=False,
    # We might end up with quite a few tasks, don't run them all at the
    # same time.
    concurrency=3,
) as dag:
    complete = DummyOperator(task_id='complete')

    refinery_drop_hive_partitions(
        task_id='drop_glent_prep_partitions',
        database='glent',
        # Each glent partition contains a full dataset, rather than for slice
        # of the time range like some other dbs. It runs once a week and uses
        # last weeks output as this weeks input, as such we really want at
        # least 2 historical outputs at any given time to allow at least some
        # backfilling.
        # The limits here are not related to privacy policy, glent is approved
        # to maintain its de-identified datasets indefinitly, but practical.
        older_than_days=29,
    ) >> complete

    refinery_drop_hive_partitions(
        task_id='drop_mjolnir_partitions',
        database='mjolnir',
        # This data is derived from long term data
        older_than_days=15,
        tables=','.join([
            # All contain private data that must not be retained.
            'feature_vectors',
            'labeled_query_page',
            'query_clicks_ltr',
            'query_clustering',
            # Intentionally excluded:
            # - model_parameters: Has no private data, useful for looking at
            #   training history.
        ])
    ) >> complete

    refinery_drop_hive_partitions(
        task_id='drop_discovery_partitions',
        older_than_days=84,  # 12 weeks
        database='discovery',
        tables=','.join([
            # Contains private data that must not be retained
            'query_clicks_daily',
            'query_clicks_hourly',
            'search_satisfaction_daily',
            # Contains non-private data, but no real need to keep forever
            'ores_articletopic',
            'ores_scores_export',
            'popularity_score',
            'wikibase_item',
            # Intentionally excluded:
            # - cirrus_namespace_index_map: Not private, not partitioned
            # - wikibase_rdf: Managed somewhere else
            # - fulltext_head_queries: Handled below with shorter allowed lifetime
        ])
    ) >> complete

    refinery_drop_hive_partitions(
        task_id='drop_discovery_short_term_partitions',
        older_than_days=6,  # Data derived from 84 day tables
        database='discovery',
        tables='fulltext_head_queries'
    ) >> complete
