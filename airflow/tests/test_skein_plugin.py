from datetime import datetime

from airflow.models.taskinstance import TaskInstance
import pytest

from wmf_airflow.skein import SkeinOperator
from wmf_airflow.swift_upload import SwiftUploadOperator

from conftest import tasks


def test_operator_can_create_hook():
    task = SkeinOperator(
        task_id='can_create_hook',
        application='pytest.py')
    assert task._make_hook() is not None


@pytest.mark.parametrize('task', list(tasks((SkeinOperator, SwiftUploadOperator))))
def test_skein_spec_against_fixtures(fixture_factory, task):
    ti = TaskInstance(task, datetime(year=2038, month=1, day=17))
    ti.render_templates()

    spec = task._make_hook()._build_spec(task._application)
    fixture = '{}-{}'.format(task.dag_id, task.task_id)
    comparer = fixture_factory('skein_operator_spec', fixture)
    comparer(spec.to_dict())
