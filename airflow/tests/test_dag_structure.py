from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models.dagbag import DagBag
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import pytest

# Failing to import a dag doesn't fail pytest, so enumerate
# the expected dags so tests fail if they don't import. This
# also helps to ignore airflow's default test dag.
all_dag_ids = [
    'mjolnir',
    'popularity_score_weekly',
    'ores_articletopic_weekly',
    'transfer_to_es_weekly',
]


def all_tasks():
    bag = DagBag()
    for dag_id in all_dag_ids:
        dag = bag.get_dag(dag_id)
        assert dag is not None, dag_id
        for task in dag.tasks:
            yield task


def tasks(kind):
    for task in all_tasks():
        if isinstance(task, kind):
            yield task


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
    # Due to how spark is packaged for wmf cluster if the python
    # is not explicitly set it can default to the ipython shell
    # which will seem to work (it will run the provided application)
    # but cli arguments will be passed to ipython instead of our
    # script.
    assert 'spark.pyspark.python' in task._conf


@pytest.mark.parametrize('dag_ids', [
    ['popularity_score_weekly', 'transfer_to_es_weekly', 'ores_articletopic_weekly'],
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
