import re
from collections import OrderedDict
from datetime import datetime
from glob import glob
import json
import os
from textwrap import dedent

from airflow.contrib.operators.spark_submit_operator \
    import SparkSubmitOperator as WrongSparkSubmitOperator
from airflow.models import Pool
from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import jinja2
import pendulum
import pytest
from wmf_airflow.spark_submit import SparkSubmitOperator

from conftest import airflow_variables_dir, all_dag_ids, all_tasks, tasks

bag = DagBag()


@pytest.mark.parametrize('dag_id', [
    dag_id for dag_id in bag.dag_ids if dag_id not in all_dag_ids
])
def test_unregistered_dag_is_example_dag(dag_id):
    dag = bag.get_dag(dag_id)
    dag_dir = os.path.basename(os.path.dirname(dag.filepath))
    # Note that dag_dir is typically the empty string for dags
    # in AIRFLOW_HOME/dags. We could have that simpler check,
    # but this seems more explicit even if the error message
    # is less obvious.
    assert dag_dir == 'example_dags', dag.dag_id


@pytest.mark.parametrize('task', tasks(WrongSparkSubmitOperator))
def test_use_spark_submit_from_our_plugin(task):
    assert 0, "Task {} is using the wrong SparkSubmitOperator implementation".format(task.task_id)


@pytest.mark.parametrize('dag_id', all_dag_ids)
def test_dag_structure(dag_id):
    # Dag must exist
    dag = bag.get_dag(dag_id)
    assert dag is not None, 'expected dag to exist: ' + dag_id
    # All dags must have a 'complete' task to ease chaining
    # dags together.
    assert any(task.task_id == 'complete' for task in dag.tasks)
    # All dags must be defined with StrictUndefined, to ensure we
    # get warnings at test time for undefined values instead of
    # empty values.
    assert dag.template_undefined == jinja2.StrictUndefined


@pytest.mark.parametrize('task', list(tasks(ExternalTaskSensor)))
def test_external_task_exists(task):
    bag = DagBag()
    assert task.external_dag_id in bag.dag_ids
    external_dag = bag.get_dag(task.external_dag_id)
    assert task.external_task_id in external_dag.task_ids
    if task.execution_date_fn is None and task.execution_delta is None:
        # It's hard to check when using custom execution times, but in the
        # default case we can verify schedule compatability by their usage of
        # equivalent string schedules.  If they used timedeltas for the
        # schedule we would have to verify start_date compatability as well.
        dag = bag.get_dag(task.dag_id)
        assert dag.schedule_interval == external_dag.schedule_interval, \
            "external task sensor has incompatible schedule"


@pytest.mark.parametrize('task', [
    t for t in tasks(ExternalTaskSensor) if t.execution_date_fn is not None
])
def test_external_task_execution_date_fn(task):
    # Simple test that we can pass a datetime and the function "works".
    # If anything complex is happening there separate tests should
    # be implemented.
    retval = task.execution_date_fn(pendulum.datetime.now())
    assert isinstance(retval, pendulum.datetime)


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


@pytest.mark.parametrize('task', tasks(HiveOperator))
def test_hive_operator_rendered_hql(task, rendered_task, fixture_factory, mocker, spark):
    if task.hql == rendered_task.hql:
        pytest.skip("hql is not templated")
        return
    fixture = '{}_{}'.format(task.dag_id, task.task_id)
    comparer = fixture_factory('hive_operator_hql', fixture, serde='str')
    # We can't simply use spark.sql(...) to verify syntax, as that would also
    # check that tables exist and have correct columns, a bit more than we are
    # setup to check.
    # Instead reach in and use the catalyst parser on its own without
    # running the analysis step that spark.sql(...) would invoke.
    # SessionState is internal to spark and has no interface stability
    # guarantees.
    jSqlParser = spark._jsparkSession.sessionState().sqlParser()
    # While splitting on ; isn't technically correct, it's correct-enough for
    # our use case. The spark parser explicitly only parses single statements
    # and bringing hive jars into the test suite seemed much too painful.
    for stmt in rendered_task.hql.split(';'):
        # bad content will throw pyspak.sql.utils.ParseException
        jSqlParser.parsePlan(stmt)

    comparer(dedent(rendered_task.hql))


@pytest.mark.parametrize('task', tasks(SparkSubmitOperator))
def test_spark_submit_cli_args_against_fixture(rendered_task, fixture_factory, mocker):
    task = rendered_task
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


@pytest.mark.parametrize('task', tasks(BashOperator))
def test_bash_commandline_against_fixture(task, fixture_factory, mocker):
    ti = TaskInstance(task, datetime(year=2038, month=1, day=17))
    ti.render_templates()

    command = task.bash_command
    # Record explicit env as well
    if task.env is not None:
        env_str = ' '.join('{}={}'.format(k, v) for k, v in task.env.items())
        command = env_str + ' ' + command

    fixture = '{}_{}'.format(task.dag_id, task.task_id)
    comparer = fixture_factory('bash_operator_command_line', fixture)
    comparer(command)


@pytest.mark.parametrize('task', tasks(SparkSubmitOperator))
def test_spark_submit_sizing(task, mocker):
    assert 'spark.dynamicAllocation.maxExecutors' in task._conf, \
        "spark.dynamicAllocation.maxExecutors should be set"
    max_exec = int(task._conf['spark.dynamicAllocation.maxExecutors'])
    exec_mem = parse_mem(task._executor_memory if task._executor_memory is not None else '1G')
    max_mem = max_exec * exec_mem
    if max_mem > 420:
        assert task.pool != Pool.DEFAULT_POOL_NAME, \
            "Large job (mem > 420g) should not be using the default pool"

    if max_mem > 800:
        assert task.pool == 'sequential', \
            "Large job (mem > 800g) should be using the sequential pool"

    exec_cores = int(task._executor_cores if task._executor_cores is not None else '1')
    assert exec_cores <= exec_mem, \
        "executor_memory looks suspiciously low (less than 1G per executor)"


@pytest.mark.parametrize('path', glob(os.path.join(airflow_variables_dir, '*.json')))
def test_airflow_config_is_valid_json(path):
    with open(path, 'rt') as f:
        variables = json.load(f)
    assert isinstance(variables, dict), "json file must contain a dict"
    assert len(variables) > 0, "json file must define at least one variable"


def _sort_items_recursive(maybe_dict):
    """Recursively sort dictionaries so iteration gives deterministic outputs"""
    if hasattr(maybe_dict, 'items'):
        items = ((k, _sort_items_recursive(v)) for k, v in maybe_dict.items())
        return OrderedDict(sorted(items, key=lambda x: x[0]))
    else:
        return maybe_dict


def parse_mem(mem_spec: str) -> int:
    """
    Parses mem specification, fails if it can find any
    Returns an int representing the mem in gigabytes
    """
    exec_mem = re.findall(r'(\d+)[Gg]', mem_spec)
    assert exec_mem, 'Mem specification is not correct'
    return int(exec_mem[0])
