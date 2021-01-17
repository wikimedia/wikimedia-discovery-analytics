"""Ship mediwaiki revision predictions to elasticsearch

mediawiki/revision/score events are constantly being generated by real time
edits. This DAG orchestrates the process of transforming the predictions into
data that will be pushed into the cirrussearch clusters. Final shipping to
elasticsearch happens in the transfer_to_es_weekly dag.

Primary input to this dag is the revision score events. To support the process
the dag also fetches two secondary inputs. It fetches up-to-date thresholds
from ORES public apis, so the thresholding stays in step with any changes on
their side without any change here. It also fetches the wikibase_item page
property for all known pages from the mariadb replicas to facilitate
propagation of predictions.

Once inputs are collected the primary transformation is applied in
prepare_mw_rev_score.py. This applies the collected thresholds to the
predictions, and propagates those predictions out using wikibase_item.
The predictions are finally formatted appropriately for elasticsearch
ingestion and stored in a table for the transfer_to_es_weekly DAG to
pick up.
"""
from datetime import datetime, timedelta
import os
from typing import List, Optional

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator

import jinja2
from wmf_airflow.hdfs_cli import HdfsCliHook
from wmf_airflow.hive_partition_range_sensor import HivePartitionRangeSensor
from wmf_airflow.skein import SkeinOperator
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import (
    HTTPS_PROXY, IVY_SETTINGS_PATH, MARIADB_CREDENTIALS_PATH,
    MEDIAWIKI_ACTIVE_DC, MEDIAWIKI_CONFIG_PATH, REPO_PATH, REPO_HDFS_PATH,
    YMD_PARTITION, DagConf, wmf_conf)


dag_conf = DagConf('ores_predictions_weekly_conf')

INPUT_TABLE = dag_conf('table_mw_rev_score')
WIKIBASE_ITEM_TABLE = dag_conf('table_wikibase_item')

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


def mw_sql_to_hive(
    task_id: str,
    sql_query: str,
    output_partition: str,
    mysql_defaults_path: str = MARIADB_CREDENTIALS_PATH,
    mediawiki_config_repo: str = MEDIAWIKI_CONFIG_PATH,
) -> SparkSubmitOperator:
    # The set of wikis to collect from, and where to find the databases
    # for those wikis, is detected through the dblist files.
    dblists = ['s{}.dblist'.format(i) for i in range(1, 9)]
    # Local paths to dblists, so we can ship to executor
    local_dblists = [os.path.join(mediawiki_config_repo, 'dblists', dblist) for dblist in dblists]

    # mysql defaults file to source username / password from. The '#'
    # tells spark to rename to the suffix when placing in working directory.
    mysql_defaults_path += '#mysql.cnf'

    return SparkSubmitOperator(
        task_id=task_id,
        # Custom environment provides dnspython dependency. The environment must come
        # from hdfs, because it has to be built on an older version of debian than runs
        # on the airflow instance.
        name='airflow: ores: ' + task_id,
        archives=REPO_HDFS_PATH + '/environments/mw_sql_to_hive/venv.zip#venv',
        py_files=REPO_PATH + '/spark/wmf_spark.py',
        # jdbc connector for talking to analytics replicas
        packages='mysql:mysql-connector-java:8.0.19',
        spark_submit_env_vars={
            # Must be explicitly provided for spark-env.sh. Note that these will not actually
            # be used by spark, it will take the override from spark.pyspark.python. This is
            # necessary to con spark-env.sh into being happy.
            'PYSPARK_PYTHON': 'python3.7',
        },
        conf={
            # Delegate retrys to airflow
            'spark.yarn.maxAppAttempts': '1',
            # Use the venv shipped in archives.
            'spark.pyspark.python': 'venv/bin/python3.7',
            # Fetch jars specified in packages from archiva
            'spark.jars.ivySettings': IVY_SETTINGS_PATH,
            # By default ivy will use $HOME/.ivy2, but system users dont have a home
            'spark.jars.ivy': '/tmp/airflow_ivy2',
            # Limit parallelism so we don't try and query 900 databases all at once
            'spark.dynamicAllocation.maxExecutors': '20',
            # Don't know exactly where it's used, but we need extra memory or we get
            # high gc and timeouts or yarn killing executors.
            'spark.executor.memoryOverhead': '1g',
            'spark.executor.memory': '4g',
        },
        files=','.join([mysql_defaults_path] + local_dblists),
        application=REPO_PATH + '/spark/mw_sql_to_hive.py',
        application_args=[
            '--mysql-defaults-file', 'mysql.cnf',
            '--dblists', ','.join(dblists),
            '--query', sql_query,
            '--output-partition', output_partition,
        ],
    )


def fetch_and_extract(
    model: str,
    output_table: str,
    propagate_from_wiki: Optional[str],
    upstream, downstream
):
    """Extract predictions for one model from mediawiki_revision_score events

    Predictions are thresholded and then prepared for passing on to convert_to_esbulk.py
    """
    thresholds_path = dag_conf('thresholds_prefix') + '/' + model + '_{{ ds_nodash }}.json'

    # Fetch per-topic thresholds from ORES to use when deciding which
    # predictions to discard as not-confident enough.
    fetch_prediction_thresholds = SkeinOperator(
        task_id='fetch_{}_prediction_thresholds'.format(model),
        application=REPO_PATH + '/spark/fetch_ores_thresholds.py',
        application_args=[
            '--model', model,
            '--output-path', 'thresholds.json',
        ],
        output_files={
            'thresholds.json': thresholds_path,
        },
        # ORES is not available from the analytics network, we need to
        # proxy to the outside world.
        env={
            'HTTPS_PROXY': HTTPS_PROXY,
        })

    if propagate_from_wiki is None:
        propagate_args: List[str] = []
    else:
        propagate_args = [
            '--wikibase-item-partition', WIKIBASE_ITEM_TABLE + '/' + YMD_PARTITION,
            '--propagate-from', propagate_from_wiki,
        ]

    # Extract the data from mediawiki event logs and put into
    # a format suitable for shipping to elasticsearch.
    extract_predictions = SparkSubmitOperator(
        task_id='extract_{}_predictions'.format(model),
        conf={
            # Delegate retrys to airflow
            'spark.yarn.maxAppAttempts': '1',
            'spark.dynamicAllocation.maxExecutors': '20',
        },
        spark_submit_env_vars={
            'PYSPARK_PYTHON': 'python3.7',
        },
        files=thresholds_path + '#thresholds.json',
        py_files=REPO_PATH + '/spark/wmf_spark.py',
        application=REPO_PATH + '/spark/prepare_mw_rev_score.py',
        application_args=propagate_args + [
            '--input-partition', INPUT_TABLE + '/@{{ ds }}/{{ macros.ds_add(ds, 7) }}',
            '--input-kind', 'mediawiki_revision_score',
            '--output-partition', output_table + '/' + YMD_PARTITION,
            '--thresholds-path', 'thresholds.json',
            '--prediction', model,
        ],
    )

    upstream >> fetch_prediction_thresholds >> extract_predictions >> downstream


# Manually triggered dag to initialize deployment
with DAG(
    'ores_predictions_init',
    default_args=default_args,
    schedule_interval='@once',
    user_defined_macros={
        'dag_conf': dag_conf.macro,
        'wmf_conf': wmf_conf.macro,
        'col_wikiid': "`wikiid` string COMMENT 'MediaWiki database name'",
        'col_page_id': "`page_id` int COMMENT 'MediaWiki page_id'",
        'col_page_namespace': "`page_namespace` int"
                              " COMMENT 'MediaWiki namespace page_id belongs to'",
        'cols_ymd': """
            `year` int COMMENT 'Year collection starts at',
            `month` int COMMENT 'Month collection starts at',
            `day` int COMMENT 'Day collection starts at'""",
    },
    template_undefined=jinja2.StrictUndefined,
) as init_dag:
    complete = DummyOperator(task_id='complete')

    HiveOperator(
        task_id='create_tables',
        hql="""
            CREATE TABLE IF NOT EXISTS {{ dag_conf.table_articletopic }} (
                {{ col_wikiid }},
                {{ col_page_id }},
                {{ col_page_namespace }},
                `articletopic` array<string> COMMENT 'ores articletopic predictions formatted as name|int_score for elasticsearch ingestion'
            )
            PARTITIONED BY ({{ cols_ymd }})
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_path_articletopic }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.table_drafttopic }} (
                {{ col_wikiid }},
                {{ col_page_id }},
                {{ col_page_namespace }},
                `drafttopic` array<string> COMMENT 'ores draftopic predictions formatted as name|int_score for elasticsearch ingestion'
            )
            PARTITIONED BY ({{ cols_ymd }})
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_path_drafttopic }}'
            ;

            -- Not used directly from dag, populated by spark/ores_bulk_ingest.py.
            -- Contains raw predictions exported by ores prior to thresholding
            -- and propagation.
            CREATE TABLE IF NOT EXISTS {{ dag_conf.table_scores_export }} (
                {{ col_page_id }},
                {{ col_page_namespace }},
                `probability` map<string,float> COMMENT 'predicted classification as key, confidence as value'
            )
            PARTITIONED BY (
                {{ col_wikiid }},
                `model` string COMMENT 'ORES model that produced predictions',
                {{ cols_ymd }}
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_path_scores_export }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.table_wikibase_item }} (
                {{ col_wikiid }},
                {{ col_page_id }},
                {{ col_page_namespace }},
                `wikibase_item` string COMMENT 'wikibase_item page property from mediawiki database'
            )
            PARTITIONED BY ({{ cols_ymd }})
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_path_wikibase_item }}'
        """  # noqa
    ) >> complete

    # Ensure the location we want to write thresholds to exists.
    PythonOperator(
        task_id='create_threshold_dir',
        python_callable=HdfsCliHook.mkdir,
        op_kwargs={
            'path': dag_conf('thresholds_prefix'),
            'parents': True
        },
        provide_context=False
    ) >> complete


with DAG(
    'ores_predictions_weekly',
    default_args=default_args,
    # Once a week at midnight on Sunday morning
    schedule_interval='0 0 * * 0',
    # As a weekly job there should never really be more than
    # one running at a time.
    max_active_runs=1,
    catchup=False,
    template_undefined=jinja2.StrictUndefined,
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
            # While multiple datacenters exist, only the currently active
            # dc is populated. Periods including switchovers will have to
            # be manually approved.
            [
                ('datacenter', MEDIAWIKI_ACTIVE_DC), ('year', None),
                ('month', None), ('day', None), ('hour', None)
            ]
        ])

    # Collect wikibase_item page props to facilitate propagating
    # predictions from enwiki to all the wikis
    extract_wikibase_items = mw_sql_to_hive(
        task_id='extract_wikibase_item',
        output_partition=WIKIBASE_ITEM_TABLE + "/" + YMD_PARTITION,
        sql_query="""
            SELECT pp_page as page_id, page_namespace, pp_value as wikibase_item
            FROM page_props
            JOIN page ON page_id = pp_page
            WHERE pp_propname="wikibase_item"
        """)

    complete = DummyOperator(task_id='complete')
    fork_per_model = DummyOperator(task_id='fork_per_model')

    [
        extract_wikibase_items,
        wait_for_data,
    ] >> fork_per_model

    fetch_and_extract(
        model='articletopic',
        output_table=dag_conf('table_articletopic'),
        propagate_from_wiki='enwiki',
        upstream=fork_per_model,
        downstream=complete)

    fetch_and_extract(
        model='drafttopic',
        output_table=dag_conf('table_drafttopic'),
        propagate_from_wiki=None,
        upstream=fork_per_model,
        downstream=complete)
