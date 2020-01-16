from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.spark_submit_plugin import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.skein_plugin import SkeinOperator
from airflow.sensors.hive_partition_range_sensor_plugin import HivePartitionRangeSensor


INPUT_TABLE = 'event.mediawiki_revision_score'
OUTPUT_TABLE = 'discovery.ores_articletopic'
WIKIBASE_ITEM_TABLE = 'discovery.wikibase_item'

PROPAGATE_FROM_WIKI = 'enwiki'
MODEL = 'articletopic'

THRESHOLDS_PATH = 'hdfs://analytics-hadoop/wmf/data/discovery/ores/thresholds/' \
    + MODEL + '/{{ ds_nodash }}.json'

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


def mw_sql_to_hive(
    task_id: str,
    sql_query: str,
    output_table: str,
    mysql_defaults_path: str = '{{ var.value.analytics_mariadb_credentials_path }}',
    # There isn't a great default here, but this will make it "just work"
    # when doing debug runs against prod cluster.
    mediawiki_config_repo: str = '/srv/mediawiki-config',
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
        # Custom environment provides dnspython dependency
        archives=REPO_BASE + '/environments/mw_sql_to_hive/venv.zip#venv',
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
            'spark.jars.ivySettings': '/etc/maven/ivysettings.xml',
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
        application=REPO_BASE + '/spark/mw_sql_to_hive.py',
        application_args=[
            '--mysql-defaults-file', 'mysql.cnf',
            '--dblists', ','.join(dblists),
            '--date', '{{ ds }}',
            '--query', sql_query,
            '--output-table', output_table,
        ],
    )


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

    # Fetch per-topic thresholds from ORES to use when collecting predictions
    fetch_prediction_thresholds = SkeinOperator(
        task_id='fetch_prediction_thresholds',
        application=REPO_BASE + '/spark/fetch_ores_thresholds.py',
        application_args=[
            '--model', MODEL,
            '--output-path', 'thresholds.json',
        ],
        output_files={
            'thresholds.json': THRESHOLDS_PATH,
        },
        # ORES is not available from the analytics network, we need to
        # proxy to the outside world.
        env={
            'HTTPS_PROXY': 'http://webproxy.eqiad.wmnet:8080',
        })

    # Collect wikibase_item page props to facilitate propagating
    # predictions from enwiki to all the wikis
    extract_wikibase_items = mw_sql_to_hive(
        task_id='extract_wikibase_item',
        output_table=WIKIBASE_ITEM_TABLE,
        sql_query="""
            SELECT pp_page as page_id, pp_value as wikibase_item
            FROM page_props
            WHERE pp_propname="wikibase_item"
        """)

    # Extract the data from mediawiki event logs and put into
    # a format suitable for shipping to elasticsearch.
    extract_predictions = SparkSubmitOperator(
        task_id='extract_predictions',
        conf={
            # Delegate retrys to airflow
            'spark.yarn.maxAppAttempts': '1',
        },
        spark_submit_env_vars={
            'PYSPARK_PYTHON': 'python3.7',
        },
        files=THRESHOLDS_PATH + '#thresholds.json',
        application=REPO_BASE + '/spark/prepare_mw_rev_score.py',
        application_args=[
            '--input-table', INPUT_TABLE,
            '--output-table', OUTPUT_TABLE,
            '--start-date', '{{ ds }}',
            '--end-date', '{{ macros.ds_add(ds, 7) }}',
            '--thresholds-path', 'thresholds.json',
            '--prediction', MODEL,
            '--wikibase-item-table', WIKIBASE_ITEM_TABLE,
            '--propagate-from', PROPAGATE_FROM_WIKI,
        ],
    )

    [
        fetch_prediction_thresholds,
        extract_wikibase_items,
        wait_for_data
    ] >> extract_predictions

    complete = DummyOperator(task_id='complete')
    extract_predictions >> complete
