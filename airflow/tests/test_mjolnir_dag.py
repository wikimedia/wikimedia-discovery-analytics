import json
import os

from airflow.models.taskinstance import TaskInstance
from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
from airflow.hooks.hive_hooks import HiveMetastoreHook
from airflow.hooks.hdfs_cli_plugin import HdfsCliHook
from airflow.operators.mjolnir_plugin import MjolnirOperator
import pytest

from conftest import dag_tasks


@pytest.mark.parametrize('task', dag_tasks('mjolnir', MjolnirOperator))
def test_spark_submit_cli_args_against_fixtures(mocker, fixture_factory, task):
    mocker.patch.object(task, '_marker_exists').return_value = False

    # TODO: Don't duplicate WIKIS list
    WIKIS = [
        'arwiki', 'dewiki', 'enwiki', 'fawiki',
        'fiwiki', 'frwiki', 'hewiki', 'idwiki',
        'itwiki', 'jawiki', 'kowiki', 'nlwiki',
        'nowiki', 'plwiki', 'ptwiki', 'ruwiki',
        'svwiki', 'viwiki', 'zhwiki',
    ]
    mocker.patch.object(HdfsCliHook, 'text').return_value = json.dumps({
        'num_obs': {k: 10000 for k in WIKIS},
        # TODO: un-hardcode 50 features
        'wiki_features': {k: [''] * 50 for k in WIKIS},
        'metadata': {
            'num_obs': 10000,
            'features': [''] * 50
        }
    })

    # Mock out metastore with some pre-defined paths
    def get_table_side_effect(database_name, table_name):
        table = mocker.MagicMock()
        table.sd.location = os.path.join(
            'hdfs://pytest/path/to', database_name, table_name)
        return table
    # mocker.patch.object(HiveMetastoreHook, 'get_connection')
    mocked_metastore = mocker.patch.object(HiveMetastoreHook, 'get_metastore_client')
    mocked_metastore().__enter__().get_table.side_effect = get_table_side_effect

    # Mock out the hook so we can collect args.
    mocked_make_spark_hook = mocker.patch.object(task, '_make_spark_hook')

    # Run the task to populate the mock with hook args
    task.dag.clear()
    ti = TaskInstance(task, task.dag.default_args['start_date'])
    ti.run(ignore_all_deps=True)

    # Fetch call args from the mocks
    assert len(mocked_make_spark_hook.call_args_list) == 1
    hook_args, hook_kwargs = mocked_make_spark_hook.call_args
    submit_args, submit_kwargs = mocked_make_spark_hook().submit.call_args

    # Create the real hook and ask for the spark cli args
    hook = SparkSubmitHook(*hook_args, **hook_kwargs)
    command = hook._build_spark_submit_command(submit_args[0])
    # Popen only accepts strings
    assert all(isinstance(x, str) for x in command), str(command)

    # Check against on-disk fixtures, or write to disk if rebuilds are enabled
    fixture = '{}_{}'.format(task.dag_id, task.task_id)
    comparer = fixture_factory('spark_submit_hook', fixture)
    comparer(command)
