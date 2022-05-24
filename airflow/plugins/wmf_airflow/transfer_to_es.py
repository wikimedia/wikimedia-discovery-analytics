from typing import Tuple, Union

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
    event_stream: Union[bool, str] = True,
) -> Tuple[BaseOperator, BaseOperator]:
    """Create operators to apply convert_to_esbulk and ship to prod

    First returned operator is convert, second is upload.  Returned operators
    are not connected, caller is responsible to set upstream and downstream as
    appropriate for their use case.

    Parameters
    ----------
    convert_config
        Named configuration of convert_to_esbulk to use
    rel_path
        Used in the output path for intermediate data. Should be unique
        per use case.
    event_stream
        Set this to False to disable sending of events. Otherwise,
        this will be used as the value of meta.stream in the produced
        swift/upload/complete event. If not set, this will default to
        swift.<container>.upload-complete.
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
            'spark.executor.memoryOverhead': '768',
            'spark.driver.memoryOverhead': '1024',
        },
        driver_memory='4g',
        executor_memory='3g',
        max_executors=25,
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
        swift_object_prefix='{{ ds_nodash }}T{{ execution_date.hour }}/' + rel_path,
        # Auto versioning will add a timestamp to the prefix, ensuring
        # retrying a task uploads to a different prefix
        swift_auto_version=True,
        event_per_object=True,
        event_stream=event_stream)

    return convert, upload
