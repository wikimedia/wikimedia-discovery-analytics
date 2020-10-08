from datetime import datetime, timedelta
from unittest.mock import MagicMock

from airflow.models.taskinstance import TaskInstance

import pendulum
from wmf_airflow.hive_partition_range_sensor import HivePartitionRangeSensor


def test_hourly():
    hook = MagicMock()
    hook.check_for_named_partition.return_value = False
    sensor = HivePartitionRangeSensor(
        task_id='pytest',
        table='table',
        period=timedelta(days=1),
        partition_frequency='hours',
        partition_specs=[
            [('year', None), ('month', None), ('day', None), ('hour', None)],
        ],
        hook=hook)
    sensor.poke({
        'execution_date': pendulum.datetime(year=2020, month=1, day=2),
    })
    assert sensor.partition_names == [
        'table/year=2020/month=1/day=2/hour={}'.format(hour) for hour in range(24)
    ]


def test_daily():
    hook = MagicMock()
    hook.check_for_named_partition.return_value = False
    sensor = HivePartitionRangeSensor(
        task_id='pytest',
        table='table',
        period=timedelta(days=7),
        partition_frequency='days',
        partition_specs=[
            [('year', None), ('month', None), ('day', None)],
        ],
        hook=hook)
    sensor.poke({
        'execution_date': pendulum.datetime(year=2020, month=1, day=2),
    })
    assert sensor.partition_names == [
        'table/year=2020/month=1/day={}'.format(day) for day in range(2, 9)
    ]


def test_date_format_is_padded():
    hook = MagicMock()
    hook.check_for_named_partition.return_value = False
    sensor = HivePartitionRangeSensor(
        task_id='pytest',
        table='table',
        period=timedelta(days=7),
        partition_frequency='days',
        partition_specs=[
            [('date', None)],
        ],
        hook=hook)
    sensor.poke({
        'execution_date': pendulum.datetime(year=2020, month=1, day=2),
    })
    assert sensor.partition_names == [
        'table/date=202001{:02d}'.format(day) for day in range(2, 9)
    ]


def test_templating(dag, mock_airflow_variables):
    mock_airflow_variables({
        'some_table': 'pytestdb.pytesttable',
        'some_partition': 'pytest',
    })
    hook = MagicMock()
    hook.check_for_named_partition.return_value = False
    sensor = HivePartitionRangeSensor(
        dag=dag,
        task_id='pytest',
        table='{{ var.value.some_table }}',
        period=timedelta(days=7),
        partition_frequency='days',
        partition_specs=[
            [('tmpl', '{{ var.value.some_partition }}'), ('date', None)],
        ],
        hook=hook)

    ti = TaskInstance(sensor, datetime(year=2038, month=1, day=17))
    ti.render_templates()

    sensor.poke({
        'execution_date': pendulum.datetime(year=2020, month=1, day=2),
    })
    assert sensor.partition_names == [
        'pytestdb.pytesttable/tmpl=pytest/date=202001{:02d}'.format(day) for day in range(2, 9)
    ]
