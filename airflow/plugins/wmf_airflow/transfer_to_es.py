from typing import Tuple

from airflow.models.baseoperator import BaseOperator

from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.swift_upload import SwiftUploadOperator
from wmf_airflow.template import DagConf


# TODO: Maybe DagConf isn't an ideal name, but for now this fits
# our usecase.
dag_conf = DagConf('transfer_to_es_conf')


def convert_and_upload(
    convert_config: str,
    rel_path: str,
) -> Tuple[BaseOperator, BaseOperator]:
    """Create operators to apply convert_to_esbulk and ship to prod

    First returned operator is convert, second is upload.  Returned operators
    are not connected, caller is responsible to set upstream and downstream as
    appropriate for their use case.
    """
    # TODO: Nothing cleans up old data on this path yet, we use date={{ ds_nodash }} in front so
    # we at least have a clear way to implement it.
    path_out = '/'.join([
        dag_conf('base_output_path'),
        'date={{ ds_nodash }}',
        rel_path
    ])

    convert = SparkSubmitOperator(
        task_id='convert_to_esbulk',
        conf={
            'spark.yarn.maxAppAttempts': '1',
            'spark.dynamicAllocation.maxExecutors': '25',
            'spark.executor.memoryOverhead': '768'
        },
        spark_submit_env_vars={
            'PYSPARK_PYTHON': 'python3.7',
        },
        py_files='{{ wmf_conf.wikimedia_discovery_analytics_path }}/spark/wmf_spark.py',
        application='{{ wmf_conf.wikimedia_discovery_analytics_path }}/spark/convert_to_esbulk.py',
        application_args=[
            '--config', convert_config,
            '--namespace-map-table', dag_conf('table_namespace_map'),
            '--output', path_out,
            '--datetime', '{{ execution_date }}',
        ])

    # Ship to production
    upload = SwiftUploadOperator(
        task_id='upload_to_swift',
        swift_container=dag_conf('swift_container'),
        source_directory=path_out,
        swift_object_prefix='{{ ds_nodash }}',
        swift_overwrite=True,
        event_per_object=True)

    return convert, upload
