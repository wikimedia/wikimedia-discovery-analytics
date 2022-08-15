from collections import defaultdict
import datetime
from typing import cast, Callable, Mapping, Sequence, Tuple

from airflow.utils.decorators import apply_defaults
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor

from pendulum import Pendulum


Formatter = Callable[[str, Pendulum], str]
PartitionSpec = Sequence[Tuple[str, str]]


class HivePartitionRangeSensor(NamedHivePartitionSensor):
    """
    The sensor waits for a range of time-based partitions to exist in hive.

    :param table: The fully-qualified hive table to check
    :param period: The time range after the execution_date to
     require partitions for.
    :param partition_frequency: The frequency partitions exist in this
     table. Must be 'hours' or 'days'.
    :param partition_specs: List of partition specifications to
     require. Each spec will be repeated for each expected partition with the
     values of the year, month, day, hour and date keys replaced.
    """
    template_fields = ('_table', '_partition_specs')

    FORMATTERS = cast(Mapping[str, Formatter], defaultdict(
        lambda: lambda v, dt: v,
        year=lambda v, dt: dt.year,
        month=lambda v, dt: dt.month,
        day=lambda v, dt: dt.day,
        hour=lambda v, dt: dt.hour,
        date=lambda v, dt: dt.format('%Y%m%d')))

    @apply_defaults
    def __init__(
        self,
        table: str,
        period: datetime.timedelta,
        partition_frequency: str,
        partition_specs: Sequence[PartitionSpec],
        partition_names=None,
        *args, **kwargs
    ):
        if partition_names is not None:
            raise TypeError('partition_names must be None')
        super().__init__(partition_names=None, *args, **kwargs)
        self._table = table
        self._period = period
        self._partition_frequency = partition_frequency
        self._partition_specs = partition_specs

    def partition_names_for_range(
        self, start_dt: Pendulum, end_dt: Pendulum
    ) -> Sequence[str]:
        if end_dt is None:
            raise TypeError('next_execution_date cannot be None')
        period = (end_dt - start_dt).range(self._partition_frequency)
        # The end date is exclusive to support end_dt as the next
        # execution date.
        if period[-1] == end_dt:
            period = period[:-1]
        partition_names = []
        for dt in period:
            for partition_spec in self._partition_specs:
                formatted = ('{}={}'.format(k, self.FORMATTERS[k](v, dt))
                             for k, v in partition_spec)
                partition_names.append(self._table + '/' + '/'.join(formatted))
        return partition_names

    def poke(self, context):
        if self.partition_names is None:
            start_dt = context['execution_date']
            end_dt = start_dt.copy().add_timedelta(self._period)
            self.partition_names = self.partition_names_for_range(
                start_dt, end_dt)
        res = super().poke(context)
        # When mode is the standard 'poke' setting we get feedback about what
        # partitions are still waiting based on subsequent pokes, but with
        # reschedule it always starts from the full list.  Help us to know
        # which partitions are missing from the logs by directly reporting them.
        if self.mode == 'reschedule' and not res:
            for partition_name in self.partition_names:
                self.log.info('Still waiting for %s', partition_name)
        return res
