"""Common templated values used across DAGs

This file primarily serves as the source of truth for what must be defined in
the wmf_conf Variable.

Typically these refer to resources deployed to the airflow host, or a fact of
the attached networks. The values are expected to be constant across dags
deployed to the same location.
"""

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

# Path to the root of this repository on hdfs. Used primarily for sourcing python
# environments that must be built in a specific context.
REPO_HDFS_PATH = wmf_conf('wikimedia_discovery_analytics_hdfs_path')

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

# MediaWiki can only be active in one datacenter at a time and eventgate inputs
# are partitioned by the datacenter they come from. This marker indicates
# which datacenter to expect events from.
MEDIAWIKI_ACTIVE_DC = wmf_conf('mediawiki_active_datacenter')

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
