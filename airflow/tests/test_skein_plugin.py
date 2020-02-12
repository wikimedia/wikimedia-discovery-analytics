from datetime import datetime

from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.operators.skein_plugin import SkeinOperator
from airflow.operators.swift_upload_plugin import SwiftUploadOperator
import pytest


def tasks(kind):
    # We create a new bag each invocation because these tasks
    # are not immutable and can be changed by tests.
    bag = DagBag()
    for dag_id in bag.dag_ids:
        dag = bag.get_dag(dag_id)
        for task in dag.tasks:
            if isinstance(task, kind):
                yield task


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
