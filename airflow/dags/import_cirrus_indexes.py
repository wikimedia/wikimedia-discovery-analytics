from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime
import jinja2
from wmf_airflow import DAG
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import REPO_PATH, DagConf, wmf_conf

dag_conf = DagConf('import_cirrus_indexes_conf')
default_args = {
    'start_date': datetime(2022, 10, 30),
}

with DAG(
    'import_cirrus_indexes_init',
    default_args=default_args,
    schedule_interval='@once',
    user_defined_macros={
        'dag_conf': dag_conf.macro,
        'wmf_conf': wmf_conf.macro,
    },
    template_undefined=jinja2.StrictUndefined
) as dag_init:
    HiveOperator(
        task_id='create_tables',
        hql="""
            CREATE TABLE {{ dag_conf.cirrus_indexes_table }} (
              `page_id` bigint,
              `auxiliary_text` array<string>,
              `category` array<string>,
              `content_model` string,
              `coordinates` array<struct<
                  `coord`: map<string, double>,
                  `country`:string,
                  `dim`:bigint,
                  `globe`:string,
                  `name`:string,
                  `primary`:boolean,
                  `region`:string,
                  `type`:string>>,
              `create_timestamp` string,
              `defaultsort` string,
              `descriptions` map<string, array<string>>,
              `external_link` array<string>,
              `extra_source` string,
              `file_bits` bigint,
              `file_height` bigint,
              `file_media_type` string,
              `file_mime` string,
              `file_resolution` bigint,
              `file_size` bigint,
              `file_text` string,
              `file_width` bigint,
              `heading` array<string>,
              `incoming_links` bigint,
              `labels` map<string, array<string>>,
              `language` string,
              `local_sites_with_dupe` array<string>,
              `namespace` bigint,
              `namespace_text` string,
              `opening_text` string,
              `outgoing_link` array<string>,
              `popularity_score` double,
              `redirect` array<struct<`namespace`:bigint,`title`:string>>,
              `source_text` string,
              `template` array<string>,
              `text` string,
              `text_bytes` bigint,
              `timestamp` string,
              `title` string,
              `version` bigint,
              `wikibase_item` string,
              `weighted_tags` array<string>,
              `statement_keywords` array<string>)
            PARTITIONED BY (
              `cirrus_replica` string,
              `cirrus_group` string,
              `wiki` string,
              `snapshot` string)
            STORED AS avro
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.cirrus_indexes_location }}'
        """
    ) >> DummyOperator(task_id='complete')


with DAG(
    'import_cirrus_indexes_weekly',
    default_args=default_args,
    user_defined_macros={
        'dag_conf': dag_conf.macro,
    },
    schedule_interval='@weekly',
    max_active_runs=1,
    catchup=False,
    template_undefined=jinja2.StrictUndefined,
) as weekly_dag:
    SparkSubmitOperator(
        task_id='import',
        conf={
            # Outputing giant rows seems to need excessive memory overhead to work
            'spark.executor.memoryOverhead': '4096M',
            # This seems to help reduce the memory overhead necessary. If too
            # small a specific exception will let us know we are using more direct
            # buffers than expected, rather than a generic killed by yarn error.
            'spark.executor.extraJavaOptions': '-XX:MaxDirectMemorySize=1024M',
        },
        # Keep down the number of connections we are making to the prod cluster
        # by limiting executors
        max_executors=36,
        executor_cores=2,
        executor_memory='4g',
        application=REPO_PATH + '/spark/import_cirrus_indexes.py',
        application_args=[
            '--elasticsearch', '{{ ",".join(dag_conf.elasticsearch_clusters) }}',
            '--output-partition', '{{ dag_conf.cirrus_indexes_table }}/snapshot={{ ds }}',
        ]
    ) >> DummyOperator(task_id='complete')
