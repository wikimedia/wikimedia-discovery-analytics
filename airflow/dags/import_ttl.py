"""Init DAG
    Create table and partitions for import_wikidata and import_commons dag
   Import Commons TTL
    Parse and munge commons TTL dumps
    - Entities dump start on sundays and takes a day
    - These dumps are copied to a cloud replica
    - The refinery job runs hdfs_rsync every days at 2:30 am
      (see puppet import_commons_mediainfo_dumps.pp)
   Import Wikidata TTL
    Parse and munge wikidata TTL dumps
    - Entities dump start on mondays and takes a couple days
    - Lexemes starts on saturdays
    - These dumps are copied to a cloud replica
    - The refinery job runs hdfs_rsync every days around 1am and 3am
      (see puppet import_wikidata_entities_dumps.pp)
"""

from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator

from wmf_airflow import DAG
from airflow.operators.hive_operator import HiveOperator
from wmf_airflow.hdfs_cli import HdfsCliSensor
from wmf_airflow.spark_submit import SparkSubmitOperator
import jinja2
import pendulum

from wmf_airflow.template import WDQS_SPARK_TOOLS, DagConf, wmf_conf

dag_conf = DagConf('import_ttl_conf')

COMMONS_DUMP_DIR = dag_conf("commons_dump_dir")
ALL_TTL_DUMP_DIR = dag_conf("all_ttl_dump_dir")
LEXEMES_TTL_DUMP_DIR = dag_conf("lexeme_ttl_dump_dir")
TRIPLE_OUTPUT_PATH = dag_conf("triple_output_path")

RDF_DATA_TABLE = dag_conf("rdf_data_table")

# Default kwargs for all Operators
default_args = {
    'start_date': datetime(2021, 2, 15),
}

with DAG(
    'import_ttl_init',
    default_args=default_args,
    schedule_interval='@once',
    user_defined_macros={
        'dag_conf': dag_conf.macro,
        'wmf_conf': wmf_conf.macro,
    },
    template_undefined=jinja2.StrictUndefined,
) as dag_init:
    complete = DummyOperator(task_id='complete')
    HiveOperator(
        task_id='create_tables',
        hql="""
            CREATE TABLE IF NOT EXISTS {{ dag_conf.rdf_data_table }} (
              `context` string,
              `subject` string,
              `predicate` string,
              `object` string
            )
            PARTITIONED BY (
                `date` string,
                `wiki` string
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rdf_data_location }}'
        """
    ) >> complete

with DAG(
    'import_commons_ttl',
    default_args=default_args,
    # commons ttl is scheduled on sundays
    # The dump arrives on cloud replica on monday
    # picked-up by the hdfs_rsync running on tuesday morning
    # Start the job on wednesdays 3am
    schedule_interval='0 3 * * 3',
    # As a weekly job there should never really be more than
    # one running at a time.
    max_active_runs=1,
    catchup=False,
    user_defined_macros={
        'p': pendulum,
    },
    template_undefined=jinja2.StrictUndefined,
) as commons_dag:
    commons_ds = "{{ execution_date.next(day_of_week=p.SUNDAY).format('%Y%m%d') }}"
    path = "%s/%s/_IMPORTED" % (COMMONS_DUMP_DIR, commons_ds)
    wiki = "commons"
    rdf_table_and_partition = '%s/date=%s/wiki=%s' % (RDF_DATA_TABLE, commons_ds, wiki)

    commons_sensor = HdfsCliSensor(task_id="wait_for_mediainfo_ttl_dump",
                                   filepath=path,
                                   poke_interval=timedelta(hours=1).total_seconds(),
                                   timeout=timedelta(days=1).total_seconds(),
                                   mode='reschedule')

    input_path = "{base}/{commons_ds}/commons-{commons_ds}-mediainfo.ttl.bz2,".format(
                 base=COMMONS_DUMP_DIR,
                 commons_ds=commons_ds,
    )

    munge_and_import_commons_dumps = SparkSubmitOperator(
        task_id='munge_dumps',
        application=WDQS_SPARK_TOOLS,
        java_class="org.wikidata.query.rdf.spark.transform.structureddata.dumps.WikibaseRDFDumpConverter",  # noqa
        max_executors=25,
        executor_cores=8,
        executor_memory="16g",
        driver_memory="2g",
        application_args=[
            '--input-path', input_path,
            '--output-table', rdf_table_and_partition,
            '--skolemize',
            '--site', wiki,
        ],
    )

    generate_entity_rev_map = SparkSubmitOperator(
        task_id='gen_rev_map',
        application=WDQS_SPARK_TOOLS,
        java_class="org.wikidata.query.rdf.spark.transform.structureddata.dumps.EntityRevisionMapGenerator",  # noqa
        max_executors=25,
        executor_cores=8,
        executor_memory="16g",
        driver_memory="2g",
        application_args=[
            '--input-table', rdf_table_and_partition,
            '--output-path', "%s/%s/rev_map.csv" % (
                dag_conf('entity_revision_map.wcqs'), commons_ds),
            '--uris-scheme', 'commons',
            '--hostname', 'commons.wikimedia.org',
        ]
    )

    end = DummyOperator(task_id='complete')
    end << generate_entity_rev_map << munge_and_import_commons_dumps << commons_sensor

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
) as wikidata_dag:
    # we have weekly runs and airflow schedules job just after the end of the period
    # an exec date on Fri Jun 5th actually means we run just after Thu Jun 12 23:59
    # but we want to wait for the dumps generated on Mon Jun 8
    all_ttl_ds = "{{ execution_date.next(day_of_week=p.MONDAY).format('%Y%m%d') }}"
    lexemes_ttl_ds = "{{ ds_nodash }}"
    path = "%s/%s/_IMPORTED" % (ALL_TTL_DUMP_DIR, all_ttl_ds)
    wiki = "wikidata"
    rdf_table_and_partition = '%s/date=%s/wiki=%s' % (RDF_DATA_TABLE, all_ttl_ds, wiki)

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

    site = "wikidata"

    munge_and_import_dumps = SparkSubmitOperator(
        task_id='munge_dumps',
        application=WDQS_SPARK_TOOLS,
        java_class="org.wikidata.query.rdf.spark.transform.structureddata.dumps.WikibaseRDFDumpConverter",  # noqa
        max_executors=25,
        executor_cores=8,
        executor_memory="16g",
        driver_memory="2g",
        application_args=[
            '--input-path', input_path,
            '--output-table', rdf_table_and_partition,
            '--skolemize',
            '--site', wiki,
        ],
    )

    generate_entity_rev_map = SparkSubmitOperator(
        task_id='gen_rev_map',
        application=WDQS_SPARK_TOOLS,
        java_class="org.wikidata.query.rdf.spark.transform.structureddata.dumps.EntityRevisionMapGenerator",  # noqa
        max_executors=25,
        executor_cores=8,
        executor_memory="16g",
        driver_memory="2g",
        application_args=[
            '--input-table', rdf_table_and_partition,
            '--output-path', "%s/%s/rev_map.csv" % (
                dag_conf('entity_revision_map.wdqs'), all_ttl_ds),
        ]
    )

    end = DummyOperator(task_id='complete')
    end << generate_entity_rev_map << munge_and_import_dumps << [all_ttl_sensor, lexeme_ttl_sensor]
