from datetime import datetime
import os

from airflow.models.taskinstance import TaskInstance
import pytest

from wmf_airflow.hdfs_to_druid import HdfsToDruidOperator

from conftest import tasks


@pytest.mark.parametrize('task', list(tasks(HdfsToDruidOperator)))
def test_hdfs_to_druid_against_fixtures(fixture_factory, mock_airflow_variables, task):
    # This path is set to a non-sensical static value for pytest
    # which ensures runs from different locations have the same
    # output.  The HdfsToDruidOperator we are invoking needs to
    # read files from the repo, so we need to put a real value
    # in there.
    mock_airflow_variables({
        'wmf_conf': {
            'wikimedia_discovery_analytics_path':
                os.path.realpath(os.path.join(__file__, '../../..'))
        }
    })
    ti = TaskInstance(task, datetime(year=2038, month=1, day=17))
    ti.render_templates()

    fixture = '{}-{}'.format(task.dag_id, task.task_id)
    comparer = fixture_factory('hdfs_to_druid_ingest_spec', fixture)
    comparer(task.index_spec)
