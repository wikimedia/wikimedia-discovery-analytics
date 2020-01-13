from airflow.models.dagbag import DagBag
from airflow.operators.swift_upload_plugin import SwiftUploadOperator
import pytest


def tasks(kind):
    bag = DagBag()
    for dag_id in bag.dag_ids:
        dag = bag.get_dag(dag_id)
        for task in dag.tasks:
            if isinstance(task, kind):
                yield task


@pytest.mark.parametrize('task', list(tasks(SwiftUploadOperator)))
def test_skein_spec_against_fixtures(fixture_factory, task):
    spec = task._make_hook()._build_spec(task._swift_upload_py)
    fixture = '{}-{}'.format(task.dag_id, task.task_id)
    comparer = fixture_factory('swift_upload_operator', fixture)
    comparer(spec.to_dict())
