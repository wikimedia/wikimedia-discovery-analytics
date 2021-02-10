"""Parse and munge wikidata TTL dumps
- Entities dump start on mondays and takes a couple days
- Lexemes starts on saturdays
- These dumps are copied to a cloud replica
- The refinery job runs hdfs_rsync every days around 1am and 3am
  (see puppet import_wikidata_entities_dumps.pp)
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

import jinja2
import pendulum
from wmf_airflow.hdfs_cli import HdfsCliSensor
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import REPO_PATH, DagConf


dag_conf = DagConf('import_wikidata_ttl_conf')

ARTIFACTS_DIR = REPO_PATH + "/artifacts"
WDQS_SPARK_TOOLS = ARTIFACTS_DIR + '/rdf-spark-tools-latest-jar-with-dependencies.jar'

ALL_TTL_DUMP_DIR = dag_conf("all_ttl_dump_dir")
LEXEMES_TTL_DUMP_DIR = dag_conf("lexeme_ttl_dump_dir")

TRIPLE_OUTPUT_PATH = dag_conf("triple_output_path")
ENTITY_REV_MAP_CSV = dag_conf("entity_revision_map")

RDF_DATA_TABLE = dag_conf("rdf_data_table")

# Default kwargs for all Operators
default_args = {
    'owner': 'discovery-analytics',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 18),
    'email': ['discovery-alerts@lists.wikimedia.org'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(hours=1),
    'provide_context': True,
}


with DAG(
    'import_wikidata_ttl',
    default_args=default_args,
    # all ttl is scheduled on mondays and lexeme on fridays
    # The all_ttl dump arrives on cloud replica on thursdays morning (5am - 7am)
    # It'll be picked-up by the hdfs_rsync running on fridays morning
    # Start the job on fridays 3am
    # we'll probably wait around ~5hours on the hdfs sensor
    schedule_interval='0 3 * * 5',
    # As a weekly job there should never really be more than
    # one running at a time.
    max_active_runs=1,
    catchup=True,
    user_defined_macros={
        'p': pendulum,
    },
    template_undefined=jinja2.StrictUndefined,
) as dag:
    # we have weekly runs and airflow schedules job just after the end of the period
    # an exec date on Fri Jun 5th actually means we run just after Thu Jun 12 23:59
    # but we want to wait for the dumps generated on Mon Jun 8 (thus the ds_add(ds, 3))
    all_ttl_ds = "{{ execution_date.next(day_of_week=p.WEDNESDAY).format('%Y%m%d') }}"
    lexemes_ttl_ds = "{{ execution_date.format('%Y%m%d') }}"
    path = "%s/%s/_IMPORTED" % (ALL_TTL_DUMP_DIR, all_ttl_ds)
    rdf_table_and_partition = '%s/date=%s' % (RDF_DATA_TABLE, all_ttl_ds)

    all_ttl_sensor = HdfsCliSensor(task_id="wait_for_all_ttl_dump",
                                   filepath=path,
                                   poke_interval=timedelta(hours=1).total_seconds(),
                                   timeout=timedelta(days=1).total_seconds(),
                                   mode='reschedule')

    path = "%s/%s/_IMPORTED" % (LEXEMES_TTL_DUMP_DIR, lexemes_ttl_ds)
    lexeme_ttl_sensor = HdfsCliSensor(task_id="wait_for_lexemes_ttl_dump",
                                      filepath=path,
                                      poke_interval=timedelta(hours=1).total_seconds(),
                                      timeout=timedelta(days=1).total_seconds(),
                                      mode='reschedule')

    input_path = "{all_base}/{all_ttl_ds}/wikidata-{all_ttl_ds}-all-BETA.ttl.bz2," \
                 "{lexemes_base}/{lexemes_ttl_ds}/" \
                 "wikidata-{lexemes_ttl_ds}-lexemes-BETA.ttl.bz2".format(
                     all_base=ALL_TTL_DUMP_DIR,
                     all_ttl_ds=all_ttl_ds,
                     lexemes_base=LEXEMES_TTL_DUMP_DIR,
                     lexemes_ttl_ds=lexemes_ttl_ds
                 )

    munge_and_import_dumps = SparkSubmitOperator(
        task_id='munge_dumps',
        conf={
            # Delegate retries to airflow
            'spark.yarn.maxAppAttempts': '1',
            'spark.dynamicAllocation.maxExecutors': '25',
        },
        application=WDQS_SPARK_TOOLS,
        java_class="org.wikidata.query.rdf.spark.WikidataTurtleDumpConverter",
        executor_cores=8,
        executor_memory="16g",
        driver_memory="2g",
        application_args=[
            '--input-path', input_path,
            '--output-table', rdf_table_and_partition,
            '--skolemize',
        ],
    )

    generate_entity_rev_map = SparkSubmitOperator(
        task_id='gen_rev_map',
        conf={
            # Delegate retries to airflow
            'spark.yarn.maxAppAttempts': '1',
            'spark.dynamicAllocation.maxExecutors': '25',
        },
        application=WDQS_SPARK_TOOLS,
        java_class="org.wikidata.query.rdf.spark.EntityRevisionMapGenerator",
        executor_cores=8,
        executor_memory="16g",
        driver_memory="2g",
        application_args=[
            '--input-table', rdf_table_and_partition,
            '--output-path', "%s/%s/rev_map.csv" % (ENTITY_REV_MAP_CSV, all_ttl_ds)
        ]
    )

    end = DummyOperator(task_id='complete')
    end << generate_entity_rev_map << munge_and_import_dumps << [all_ttl_sensor, lexeme_ttl_sensor]
