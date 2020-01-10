from datetime import timedelta
from unittest.mock import MagicMock

from airflow.sensors.hive_partition_range_sensor_plugin import HivePartitionRangeSensor
import pendulum


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
