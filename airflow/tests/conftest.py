from copy import deepcopy
from datetime import datetime
from dataclasses import dataclass
from glob import glob
import json
import logging
import os
from typing import Any, Callable, TextIO

from airflow import macros
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable

import jinja2
import pytest

import findspark
findspark.init()  # must happen before importing pyspark
from pyspark.sql import SparkSession  # noqa


# Failing to import a dag doesn't fail pytest. Enumerate
# the expected dags so tests fail if they don't import.
# Unlisted non-example dags will fail the test suite.
all_dag_ids = [
    'cirrus_namespace_map_daily',
    'drop_old_data_daily',
    'fulltext_head_queries_daily',
    'glent_weekly',
    'import_wikidata_ttl',
    'mediawiki_revision_recommendation_create_init',
    'mediawiki_revision_recommendation_create_hourly',
    'mjolnir',
    'ores_predictions_v3_init',
    'ores_predictions_hourly',
    'ores_predictions_daily',
    'ores_predictions_wbitem',
    'popularity_score_weekly',
    'search_satisfaction_daily',
    'transfer_to_es_hourly',
    'transfer_to_es_weekly',
    'mediawiki_revision_recommendation_create_hourly',
]


fixture_dir = os.path.join(os.path.dirname(__file__), 'fixtures')
airflow_variables_dir = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '../config'))
dag_bag = DagBag()


@pytest.fixture(scope='session', autouse=True)
def configure_airflow_variables():
    """Global airflow variable configuration for tests.

    This configuration is written to the airflow database, and as
    such is shared between all tests. Individual tests should never
    use Variable.set, as the state crosses test boundaries. See
    mock_airflow_variables.
    """
    from airflow.bin.cli import import_helper
    for path in glob(os.path.join(airflow_variables_dir, '*.json')):
        import_helper(path)


@dataclass
class FixtureSerDe:
    encode: Callable[[TextIO, Any], None]
    decode: Callable[[TextIO], Any]


def on_disk_fixture(path, serde: FixtureSerDe):
    def compare(other):
        if os.path.exists(path):
            with open(path, 'r') as f:
                expect = serde.decode(f)
            assert expect == other
        elif os.environ.get('REBUILD_FIXTURES') == 'yes':
            with open(path, 'w') as f:
                serde.encode(f, other)
            pytest.skip("Rebuilt fixture")
        else:
            raise Exception('No fixture [{}] and REBUILD_FIXTURES != yes'.format(path))
    return compare


on_disk_fixture.serde = {
    'json': FixtureSerDe(
        encode=lambda f, val: json.dump(val, f, indent=4, sort_keys=True),
        decode=lambda f: json.load(f)),
    'str': FixtureSerDe(
        encode=lambda f, val: f.write(val),
        decode=lambda f: f.read())
}


@pytest.fixture(scope='session')
def fixture_factory():
    def factory(group, fixture_id, serde='json'):
        path = os.path.join(fixture_dir, group, fixture_id + '.expected')
        return on_disk_fixture(path, on_disk_fixture.serde[serde])
    return factory


def all_tasks():
    for dag_id in all_dag_ids:
        dag = dag_bag.get_dag(dag_id)
        if dag is None:
            # This is run while collecting tests, before running them.
            # Failing here would bail the entire suite. There is a
            # separate test that will check all_dag_ids and fail.
            continue
        for task in dag.tasks:
            yield task


def tasks(kind):
    return [task for task in all_tasks() if isinstance(task, kind)]


def dag_tasks(dag_id, kind):
    dag = dag_bag.get_dag(dag_id)
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


def quiet_log4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a spark context.

    Args:
        request: pytest.FixtureRequest object

    Returns:
        SparkContext for tests
    """

    quiet_log4j()

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pyspark-local-testing")
        # By default spark will shuffle to 200 partitions, which is
        # way too many for our small test cases. This cuts execution
        # time of the tests in half.
        .config('spark.sql.shuffle.partitions', 4)
    )
    if 'XDG_CACHE_HOME' in os.environ:
        builder.config('spark.jars.ivy', os.path.join(os.environ['XDG_CACHE_HOME'], 'ivy2'))

    with builder.getOrCreate() as spark:
        yield spark


# Some dags depend on dag_run.conf having values. Provide
# them when needed for generic fixture based testing.
DAG_RUN_CONF = {
    'top_fulltext_queries_for_wiki': {
        'wiki': 'pytestwiki',
    },
}


@pytest.fixture
def rendered_task(task, mocker):
    # This will try and talk to hive, can't let it. And yes, it really
    # returns a binary string.
    mocker.patch.object(macros.hive, 'max_partition').return_value = b'20010115'
    # This will change the task, take a copy
    task = deepcopy(task)
    # Some dags have expectations, such as it always runs on sunday. Some date manipulation
    # can depend on this. To ensure changes to date manipulation are properly tested for
    # consistency render the task against the first run date of the task.
    task_first_dt = dag_bag.get_dag(task.dag_id).normalize_schedule(task.start_date)
    ti = TaskInstance(task, task_first_dt)
    context = ti.get_template_context()
    context['run_id'] = 'pytest_compare_against_fixtures'
    if task.dag_id in DAG_RUN_CONF:
        context['dag_run'] = mocker.MagicMock()
        context['dag_run'].conf = DAG_RUN_CONF[task.dag_id]
    ti.render_templates(context=context)
    return task
