from datetime import datetime
import shlex
from typing import Sequence

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from wmf_airflow import DAG
from wmf_airflow.template import REPO_PATH, ANALYTICS_REFINERY_PATH


def refinery_drop_older_than(
    database: str,
    tables: Sequence[str],
    # This script requires a significant arguments checksum. A human must
    # execute the script without a checksum, verify the dry-run operation, and
    # update the checksum reported in any automated usage.
    checksum: str,
    older_than_days: int = 60,
    *args, **kwargs
):
    # Refinery isn't packaged like our own python, so it's hard
    # to invoke remotely with skein. Simply execute it locally
    # with a venv that has the appropriate dependencies.
    executable = '/usr/bin/env'
    arguments = [
        'PYTHONPATH={}/python/'.format(ANALYTICS_REFINERY_PATH),
        REPO_PATH + '/environments/refinery/venv/bin/python',
        ANALYTICS_REFINERY_PATH + '/bin/refinery-drop-older-than',
        '--verbose',
        '--database=' + database,
        '--tables=^({})$'.format('|'.join(tables)),
        '--older-than=' + str(older_than_days),
        '--execute=' + checksum,
    ]

    safe_arguments = ' '.join(shlex.quote(x) for x in arguments)
    bash_command = executable + ' ' + safe_arguments

    return BashOperator(bash_command=bash_command, *args, **kwargs)


with DAG(
    'drop_old_data_daily',
    default_args={
        'start_date': datetime(2020, 7, 22),
    },
    schedule_interval='@daily',
    max_active_runs=2,
    # Always deletes items older than X days, rather than based on execution
    # date. catching up missed runs would do nothing.
    catchup=False,
    # We might end up with quite a few tasks, don't run them all at the
    # same time.
    concurrency=3,
) as dag:
    complete = DummyOperator(task_id='complete')

    refinery_drop_older_than(
        task_id='drop_glent_prep_partitions',
        database='glent',
        tables=['.*'],
        # Each glent partition contains a full dataset, rather than for slice
        # of the time range like some other dbs. It runs once a week and uses
        # last weeks output as this weeks input, as such we really want at
        # least 2 historical outputs at any given time to allow at least some
        # backfilling.
        # The limits here are not related to privacy policy, glent is approved
        # to maintain its de-identified datasets indefinitly, but practical.
        older_than_days=29,
        checksum='66e1a6a9afff6fa7a76e3b25f611287d',
    ) >> complete

    refinery_drop_older_than(
        task_id='drop_mjolnir_partitions',
        database='mjolnir',
        # This data is derived from long term data
        older_than_days=15,
        tables=[
            # All contain private data that must not be retained.
            'feature_vectors',
            'labeled_query_page',
            'query_clicks_ltr',
            'query_clustering',
            # Intentionally excluded:
            # - model_parameters: Has no private data, useful for looking at
            #   training history.
        ],
        checksum='fd1a8418fbe95f56beeaf0ddc5e81fbe',
    ) >> complete

    refinery_drop_older_than(
        task_id='drop_discovery_partitions',
        older_than_days=84,  # 12 weeks
        database='discovery',
        tables=[
            # Contains private data that must not be retained
            'query_clicks_daily',
            'query_clicks_hourly',
            'search_satisfaction_daily',
            # Contains non-private data, but no real need to keep forever
            'mediawiki_revision_recommendation_create',
            'ores_articletopic',
            'ores_scores_export',
            'popularity_score',
            'wikibase_item',
            # Intentionally excluded:
            # - cirrus_namespace_index_map: Not private, not partitioned
            # - wikibase_rdf: Managed somewhere else
            # - fulltext_head_queries: Handled below with shorter allowed lifetime
        ],
        checksum='bd4c3f327371196380bb574cba281d09',
    ) >> complete

    refinery_drop_older_than(
        task_id='drop_discovery_short_term_partitions',
        older_than_days=6,  # Data derived from 84 day tables
        database='discovery',
        tables=['fulltext_head_queries'],
        checksum='bc4207e7033413997a69e3473b0d6f5f',
    ) >> complete

    refinery_drop_older_than(
        task_id='drop_wikibase_rdf_partitions',
        database='discovery',
        tables=["wikibase_rdf"],
        # we want to keep 4 partition (generated weekly). But since the data takes
        # multiple days to arrive we allow a 6 days tolerance here
        older_than_days=29 + 6,
        checksum='b51f3fa319fc7bf71e3bc7128c855b4b',
    ) >> complete

    refinery_drop_older_than(
        task_id='drop_processed_external_sparql_query_partitions',
        database='discovery',
        tables=["processed_external_sparql_query"],
        older_than_days=90,
        checksum='6fa0b352b56bae3b76e61794e1a43611',
    ) >> complete
