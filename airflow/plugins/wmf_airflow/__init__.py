import airflow
from datetime import timedelta
import jinja2
import pendulum
from wmf_airflow.template import wmf_conf

__all__ = ['DAG']


dag_defaults = {
    'default_args': {
        'owner': 'discovery-analytics',
        'depends_on_past': False,
        'email': ['discovery-alerts@lists.wikimedia.org'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=15),
        'provide_context': True,
    },
    'user_defined_macros': {
        'wmf_conf': wmf_conf.macro,
        # For use in datetime manipulation, such as dt.next(p.WEDNESDAY)
        'p': pendulum,
    },
    'template_undefined': jinja2.StrictUndefined,
}


def _merge(default, provided):
    """Override default keys with provided if both are dicts, otherwise return provided"""
    if isinstance(provided, dict) and isinstance(default, dict):
        return dict(default, **provided)
    else:
        return provided


def DAG(dag_id, **kwargs) -> airflow.DAG:
    """Standardized initialization of DAG

    Provide common defaults used in this repositories dag's to reduce per-dag
    boilerplate.

    While slightly misleading that this is named DAG, airflow requires
    the strings `airflow` and `DAG` to exist in .py files or it wont try
    and parse them in the scheduler. Avoid mysterious failures by having
    the expected name.
    """
    for k, default_value in dag_defaults.items():
        if k in kwargs:
            kwargs[k] = _merge(default_value, kwargs[k])
        else:
            kwargs[k] = default_value

    return airflow.DAG(dag_id, **kwargs)
