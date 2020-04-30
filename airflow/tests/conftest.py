import json
import os

from airflow.models.dagbag import DagBag
import pytest

# Failing to import a dag doesn't fail pytest, so enumerate
# the expected dags so tests fail if they don't import. This
# also helps to ignore airflow's default test dag.
all_dag_ids = [
    'mjolnir',
    'ores_predictions_weekly',
    'popularity_score_weekly',
    'transfer_to_es_weekly',
]


@pytest.fixture
def fixture_dir():
    return os.path.join(os.path.dirname(__file__), 'fixtures')


def on_disk_fixture(path):
    def compare(other):
        if os.path.exists(path):
            with open(path, 'r') as f:
                expect = json.load(f)
            assert expect == other
        elif os.environ.get('REBUILD_FIXTURES') == 'yes':
            with open(path, 'w') as f:
                json.dump(other, f, indent=4, sort_keys=True)
            pytest.skip("Rebuilt fixture")
        else:
            raise Exception('No fixture [{}] and REBUILD_FIXTURES != yes'.format(path))
    return compare


@pytest.fixture
def fixture_factory(fixture_dir):
    def factory(group, fixture_id):
        path = os.path.join(fixture_dir, group, fixture_id + '.expected')
        return on_disk_fixture(path)
    return factory


def all_tasks():
    bag = DagBag()
    for dag_id in all_dag_ids:
        dag = bag.get_dag(dag_id)
        assert dag is not None, dag_id
        for task in dag.tasks:
            yield task


def tasks(kind):
    return [task for task in all_tasks() if isinstance(task, kind)]


def dag_tasks(dag_id, kind):
    dag = DagBag().get_dag(dag_id)
    return [task for task in dag.tasks if isinstance(task, kind)]
