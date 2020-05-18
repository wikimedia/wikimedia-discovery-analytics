from collections import OrderedDict
from datetime import datetime

from airflow.contrib.operators.spark_submit_operator \
    import SparkSubmitOperator as WrongSparkSubmitOperator
from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import pytest
from wmf_airflow.spark_submit import SparkSubmitOperator

from conftest import all_dag_ids, all_tasks, tasks


@pytest.mark.parametrize('task', tasks(WrongSparkSubmitOperator))
def test_use_spark_submit_from_our_plugin(task):
    assert 0, "Task {} is using the wrong SparkSubmitOperator implementation".format(task.task_id)


@pytest.mark.parametrize('dag_id', all_dag_ids)
def test_dag_structure(dag_id):
    # Dag must exist
    dag = DagBag().get_dag(dag_id)
    # All dags must have a 'complete' task to ease chaining
    # dags together.
    assert any(task.task_id == 'complete' for task in dag.tasks)


@pytest.mark.parametrize('task', list(tasks(ExternalTaskSensor)))
def test_external_task_exists(task):
    bag = DagBag()
    assert task.external_dag_id in bag.dag_ids
    external_dag = bag.get_dag(task.external_dag_id)
    assert task.external_task_id in external_dag.task_ids


EMAIL_WHITELIST = {
    'ebernhardson@wikimedia.org',
    'discovery-alerts@lists.wikimedia.org',
}


@pytest.mark.parametrize('task', all_tasks())
def test_task_email_is_whitelisted(task):
    """Help prevent typos in alerting emails"""
    for email in task.email:
        assert email in EMAIL_WHITELIST


@pytest.mark.parametrize('task', tasks(SparkSubmitOperator))
def test_spark_submit_sets_python_version(task):
    # Don't check python versions on java tasks.
    if task._application.endswith('.jar'):
        return
    # Due to how spark is packaged for wmf cluster if the python
    # is not explicitly set it can default to the ipython shell
    # which will seem to work (it will run the provided application)
    # but cli arguments will be passed to ipython instead of our
    # script.
    has_conf = 'spark.pyspark.python' in task._conf
    try:
        has_env = 'PYSPARK_PYTHON' in task._spark_submit_env_vars
    except TypeError:
        has_env = False

    if not has_conf and not has_env:
        assert 0, "SparkSubmitOperator must have python executable explicitly provided"

    elif has_conf and not has_env:
        # If you only specify the python version through spark.pyspark.python,
        # and not also through PYSPARK_PYTHON, the wmf spark-env.sh script will
        # not know what version of python dependencies to load.
        assert 0, "SparkSubmitOperator cannot specify custom python only through spark conf"

    elif has_env and not has_conf:
        # If we specify python through the environment variables it will be
        # correctly detected by spark-env.sh, but we can only refer to python
        # executables that are valid on the airflow instance, even though the
        # spark app will not be run there. Basically we can only refer to
        # system python versions this way. We only allow specific python
        # versions because the `python3` executable may be 3.5 on the machine
        # deployed to, but 3.7 on the airflow host.
        # Note that only python3.7 is available on an-airflow1001 (w/ debian buster)
        assert task._spark_submit_env_vars['PYSPARK_PYTHON'] == 'python3.7'

    elif has_env and has_conf:
        # To run python with our own custom environments we need to
        # hack around the wmf adjusted spark-env.sh script. This is specific to
        # using spark deploy-mode=cluster, but that's how spark is configured
        # in wmf airflow.
        # The PYSPARK_PYTHON env var must be set to a python executable that
        # works on the airflow instance, it cannot refer to the real
        # environment. It is invoked by spark-env.sh on the instance running
        # spark-submit to determine which version of pyspark dependencies to
        # include in PYTHONPATH. The spark.pyspark.python configuration
        # variable must point to the real executable on the driver/executors,
        # this will override the PYSPARK_PYTHON environment variable.
        assert task._spark_submit_env_vars['PYSPARK_PYTHON'] == 'python3.7'
        assert task._conf['spark.pyspark.python'].endswith("/python3.7")
    else:
        # two booleans, 4 conditions, this must be unreachable.
        assert 0, "Unreachable"

    # As long as PYSPARK_PYTHON env var is set spark-env.sh will not try and
    # set PYSPARK_DRIVER_PYTHON, We should not set it as it will only confuse
    # things. spark.pyspark.driver.python can still be configured to change the
    # executable used on the driver.
    assert 'PYSPARK_DRIVER_PYTHON' not in task._spark_submit_env_vars


@pytest.mark.parametrize('task', tasks(SparkSubmitOperator))
def test_spark_submit_cli_args_against_fixture(task, fixture_factory, mocker):
    ti = TaskInstance(task, datetime(year=2038, month=1, day=17))
    ti.render_templates()

    # The conf dict comes out in random order...sort for stable cli command output
    # Must come after rendering templates, as they may replace or re-order the dict.
    if task._conf is not None:
        task._conf = _sort_items_recursive(task._conf)

    command = task._make_hook()._build_spark_submit_command(task._application)
    # Prefix the recorded fixture with the extra environment to record changes
    # there as well.
    if task._spark_submit_env_vars:
        env_str = ['{}={}'.format(k, v) for k, v in task._spark_submit_env_vars.items()]
        command = env_str + command
    assert all(isinstance(x, str) for x in command), str(command)

    fixture = '{}_{}'.format(task.dag_id, task.task_id)
    comparer = fixture_factory('spark_submit_operator', fixture)
    comparer(command)


@pytest.mark.parametrize('dag_ids', [
    ['popularity_score_weekly', 'transfer_to_es_weekly', 'ores_predictions_weekly'],
])
def test_compatible_schedules(dag_ids):
    dagbag = DagBag()
    dags = [dagbag.get_dag(dag_id) for dag_id in dag_ids]

    head = dags[0]
    # To ensure schedule compatability dags must be using
    # cron expressions. If they used timedeltas we would
    # have to verify start_date compatability as well.
    assert isinstance(head.schedule_interval, str)
    for dag in dags[1:]:
        assert head.schedule_interval == dag.schedule_interval


def _sort_items_recursive(maybe_dict):
    """Recursively sort dictionaries so iteration gives deterministic outputs"""
    if hasattr(maybe_dict, 'items'):
        items = ((k, _sort_items_recursive(v)) for k, v in maybe_dict.items())
        return OrderedDict(sorted(items, key=lambda x: x[0]))
    else:
        return maybe_dict
