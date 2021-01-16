from datetime import datetime
from glob import glob
import json
import os

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.variable import Variable

import jinja2
import pytest

# Failing to import a dag doesn't fail pytest, so enumerate
# the expected dags so tests fail if they don't import. This
# also helps to ignore airflow's default test dag.
all_dag_ids = [
    'cirrus_namespace_map_daily',
    'drop_old_data_daily',
    'fulltext_head_queries_daily',
    'glent_weekly',
    'import_wikidata_ttl',
    'mjolnir',
    'ores_predictions_weekly',
    'popularity_score_weekly',
    'search_satisfaction_daily',
    'transfer_to_es_weekly',
]


@pytest.fixture(scope='session')
def fixture_dir():
    return os.path.join(os.path.dirname(__file__), 'fixtures')


@pytest.fixture(scope='session')
def airflow_variables_dir():
    return os.path.realpath(
        os.path.join(os.path.dirname(__file__), '../config'))


@pytest.fixture(scope='session', autouse=True)
def configure_airflow_variables(airflow_variables_dir):
    """Global airflow variable configuration for tests.

    This configuration is written to the airflow database, and as
    such is shared between all tests. Individual tests should never
    use Variable.set, as the state crosses test boundaries. See
    mock_airflow_variables.
    """
    from airflow.bin.cli import import_helper
    for path in glob(os.path.join(airflow_variables_dir, '*.json')):
        import_helper(path)


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


@pytest.fixture(scope='session')
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


@pytest.fixture
def mock_airflow_variables(mocker):
    def mock_variables(mapping):
        Variable_get = Variable.get

        def mock_Variable_get(key, *args, **kwargs):
            try:
                # This isn't strictly true, we are ignoring kwargs like
                # deserialize_json and assume the test provided mapping
                # as it's desired return value.
                return mapping[key]
            except KeyError:
                return Variable_get(key, *args, **kwargs)
        mocker.patch.object(Variable, 'get').side_effect = mock_Variable_get
    return mock_variables


@pytest.fixture
def dag():
    """Simple dag object pre-populated to allow testing operators"""
    return DAG(
        dag_id='pytest',
        template_undefined=jinja2.StrictUndefined,
        default_args={'start_date': datetime.now()}
    )
