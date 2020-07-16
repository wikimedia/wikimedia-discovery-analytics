"""Common templated values used across DAGs

This file serves as the source of truth for what must be defined in the
wmf_conf Variable.

Typically these refer to resources deployed to the airflow host, or a fact of
the attached networks. The values are expected to be constant across dags
deployed to the same location.
"""


def wmf_conf(key: str) -> str:
    return '{{ var.json.wmf_conf.%s }}' % key


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
