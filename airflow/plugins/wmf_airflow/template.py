"""Common templated values used across DAGs

This file serves as the source of truth for what must be defined in
the wmf_conf Variable. It also contains helpers for common templated
use cases in the repo.

Typically the wmf_conf variables refer to resources deployed to the airflow
host, or a fact of the attached networks. The values are expected to be
constant across dags deployed to the same location.
"""
from typing import Callable, Optional, Sequence, Tuple

from airflow.models.variable import Variable


class DagConf:
    """

    Centralizes conventions around accessing variables stored
    and deployed from /airflow/config/*.json
    """
    def __init__(self, var_name: str):
        self.var_name = var_name

    def __call__(self, item):
        return '{{ var.json.%s.%s }}' % (self.var_name, item)

    @property
    def macro(self):
        """Accessor suitable for use as template macro

        Register in DAG:
            dag_conf = DagConf('...')
            ...
            DAG(..., user_defined_macros=dict(dag_conf=dag_conf.macro))
        Access in templates:
            {{ dag_conf.my_table }}
        """
        return LazyJsonVariableAccessor(self.var_name)


wmf_conf = DagConf('wmf_conf')


# Local path to the root of this repository (wikimedia/discovery/analytics) on
# the airflow server
REPO_PATH = wmf_conf('wikimedia_discovery_analytics_path')

# Local path to binary artifacts (wheels/jars/...)
ARTIFACTS_DIR = REPO_PATH + "/artifacts"

# Path to the jar containing WDQS/WCQS spark jobs
WDQS_SPARK_TOOLS = ARTIFACTS_DIR + '/rdf-spark-tools-latest-jar-with-dependencies.jar'

HTTPS_PROXY = wmf_conf('https_proxy')

# Local path to ivysettings.xml on the airflow server that specifies the remote
# jar repository spark applications should use.
IVY_SETTINGS_PATH = wmf_conf('ivy_settings_path')

# Local path to credentials for the mariadb replicas on the airflow server
MARIADB_CREDENTIALS_PATH = wmf_conf('mariadb_credentials_path')

# Local path to the operations/mediawiki-config repository on the airflow server
MEDIAWIKI_CONFIG_PATH = wmf_conf('mediawiki_config_path')

# Local path to the analytics/refinery repository on the airflow server
ANALYTICS_REFINERY_PATH = wmf_conf('analytics_refinery_path')

# execution date formatted as hive partition with year=/month=/day=/hour=
YMDH_PARTITION = \
    'year={{ execution_date.year }}/month={{ execution_date.month }}/' \
    'day={{ execution_date.day }}/hour={{ execution_date.hour }}'

# execution date formatted as hive partition with year=/month=/day=
YMD_PARTITION = \
    'year={{ execution_date.year }}/month={{ execution_date.month }}/' \
    'day={{ execution_date.day }}'


class LazyJsonVariableAccessor:
    """Lazy double of json dict stored in airflow Variable

    This is quite similar to VariableJsonAccessor, the var.json impl, from
    airflow.models.taskinstance.TaskInstance.get_template_context,
    but provides more concise access to variables commonly accessed.

    The DAG can be defined with a LazyJsonVariableAccessor in
    user_defined_macros for it's config. This is implemented lazily
    to avoid fetching the variable on all DAG evaluations.

    With accessor:
      {{ conf.my_table }}
    Without accessor:
      {{ var.json.my_dag_name_conf.my_table }}
    """
    def __init__(self, var_name: str):
        self.var = None
        self.var_name = var_name

    def __getattr__(self, item):
        if self.var is None:
            self.var = Variable.get(self.var_name, deserialize_json=True)
        try:
            return self.var[item]
        except KeyError:
            raise AttributeError('Attribute not found in ' + self.var_name)


class TemplatedSeq:
    """Dynamic length templated sequence

    Typically sequences passed to an airflow operator have to be a constant
    length. This wrapper interprets a templated string as a delimited sequence
    of elements, allowing to generate a dynamic length sequence as an operator
    field value.

    During template rendering, performed prior to task display or execution,
    airflow will recurse from the task into templated fields containing a
    TemplatedSeq and render our template_fields. Prior to rendering the output
    of the sequence is undefined. In practice this is acceptable as field
    values are unreferenced outside of task execution, and templates are always
    rendered prior to task execution or display.
    """
    template_fields = ('fn_args', 'template')

    def __init__(
        self,
        template: str,
        fn_args: Optional[Sequence] = None,
        fn: Optional[Callable] = None,
        sep: str = ',',
    ):
        self.template = template
        self.fn_args = tuple() if fn_args is None else fn_args
        self.fn = (lambda x: x) if fn is None else fn
        self.sep = sep

    @classmethod
    def for_var(
        cls,
        var_expr: str,
        fn_args: Optional[Sequence] = None,
        fn: Optional[Callable] = None,
        sep: str = ','
    ):
        """Specialize TemplatedSeq to wrap values of a single config list"""
        template = '{{ ' + var_expr + '|join("' + sep + '") }}'
        return cls(template, fn_args, fn, sep)

    def __str__(self):
        # This is a slight lie, but it's how we intend to use it and helps visibility
        # in the 'rendered' tab of a task instance where this is displayed.
        return str(list(self))

    def __repr__(self):
        return "<TemplatedSeq: {}>".format(self.template)

    def __iter__(self):
        """Returns an iterable over the templated sequence.

        Assumes the template has been rendered into it's final form, output is
        undefined prior to rendering. The inclusion of an output transformation
        allows the creation of complex return values, rather than only strings.
        """
        return (self.fn(x, *self.fn_args) for x in self.template.split(self.sep))


def eventgate_partitions(table_tmpl: str,
                         datetime_partition: str = YMDH_PARTITION) -> Sequence[str]:
    """Templated sequence of hive partition spec strings for one hour of eventgate input

    Eventgate inputs are consistent across use cases. They come with a
    partition for every active datacenter, and have the same hourly
    partitioning scheme across all tables.

    By default, it extracts hourly data. If datetime_partition is provided,
    it can extract daily or monthly data.

    Input table is a templated string. Output suitable for use in
    NamedHivePartitionSensor.partition_names
    """
    return TemplatedSeq.for_var(
        'wmf_conf.eventgate_datacenters',
        fn_args=(table_tmpl, datetime_partition),
        fn=lambda dc, table, dt: '{}/datacenter={}/{}'.format(table, dc, dt))


def eventgate_partition_range() -> Sequence[Sequence[Tuple]]:
    """Constant partition specification for time ranged eventgate tables

    Output suitable for use in HivePartitionRangeSensor.partition_specs
    """
    return TemplatedSeq.for_var(
        'wmf_conf.eventgate_datacenters',
        fn=lambda dc: [
            ('datacenter', dc), ('year', None),
            ('month', None), ('day', None), ('hour', None)
        ])
