from datetime import datetime, timedelta

from airflow.models.taskinstance import TaskInstance
import pytest

from wmf_airflow.swift_upload import SwiftUploadOperator


@pytest.mark.parametrize('delete_after', [
    '60',
    '{{ macros.timedelta(minutes=1).total_seconds() }}',
    timedelta(minutes=1)
])
def test_delete_after(dag, delete_after):
    op = SwiftUploadOperator(
        dag=dag, task_id='a', swift_container='', source_directory='',
        swift_object_prefix='', swift_delete_after=delete_after)
    ti = TaskInstance(op, datetime(year=2038, month=1, day=17))
    ti.render_templates()

    assert op._swift_delete_after_sec == 60
