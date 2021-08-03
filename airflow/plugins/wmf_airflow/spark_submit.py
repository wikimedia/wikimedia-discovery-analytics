import os

from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from wmf_airflow.template import REPO_PATH


class SparkSubmitOperator(BaseOperator):
    """
    This operator provides a few minor improvements over the SparkSubmitOperator shipped
    with airflow. In particular:

    * Due to the spark-env.sh shipped to WMF hadoop cluster, we need to be able to set the
     environment spark-submit is executed with to override it's behaviour. SparkSubmitHook
     provides this functionality, but the upstream SparkSubmitOperator cannot invoke it.

     * The _archives field was added to the set of template_fields
     * Applies the provided env_vars to the executors. Upstream only applies them
       to the master.
     * Add max_executors kwarg as shortcut to setting spark.dynamicAllocation.maxExecutors
       conf value.
     * Defaults spark.yarn.maxAppAttempts to 1 to delegate retries to airflow and keep the
       mental model of how many times a script is run simple.
     * Add python kwarg. When a string it treats as path to packaged venv which will be
       used as the runtime. When truthy enables discolytics defaults for running python.
       False does nothing. None (default) autodetects True or False by application
       extension.
    """
    template_fields = (
        '_application', '_conf', '_files', '_py_files', '_jars',
        '_driver_class_path', '_packages', '_exclude_packages', '_keytab',
        '_principal', '_proxy_user', '_name', '_application_args', '_env_vars',
        '_archives')

    @apply_defaults
    def __init__(self,
                 application='',
                 conf=None,
                 conn_id='spark_default',
                 files=None,
                 py_files=None,
                 archives=None,
                 driver_class_path=None,
                 jars=None,
                 java_class=None,
                 packages=None,
                 exclude_packages=None,
                 repositories=None,
                 total_executor_cores=None,
                 max_executors=None,
                 executor_cores=None,
                 executor_memory=None,
                 driver_memory=None,
                 keytab=None,
                 principal=None,
                 proxy_user=None,
                 name='airflow-spark',
                 num_executors=None,
                 application_args=None,
                 env_vars=None,
                 spark_submit_env_vars=None,
                 verbose=False,
                 spark_binary="spark-submit",
                 python=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._application = application
        self._conf = conf.copy() if conf else {}
        self._files = files
        self._py_files = py_files
        self._archives = archives
        self._driver_class_path = driver_class_path
        self._jars = jars
        self._java_class = java_class
        self._packages = packages
        self._exclude_packages = exclude_packages
        self._repositories = repositories
        self._total_executor_cores = total_executor_cores
        self._max_executors = max_executors
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._keytab = keytab
        self._principal = principal
        self._proxy_user = proxy_user
        self._name = name
        self._num_executors = num_executors
        self._application_args = application_args
        self._env_vars = env_vars.copy() if env_vars else {}
        self._spark_submit_env_vars = spark_submit_env_vars.copy() if spark_submit_env_vars else {}
        self._verbose = verbose
        self._spark_binary = spark_binary
        self._python = python if python is not None else application.endswith('.py')
        self._hook = None
        self._conn_id = conn_id

        def append(base, val, sep=','):
            if base is None:
                return val
            else:
                return base + sep + val

        # Delegate retrys to airflow.
        if 'spark.yarn.maxAppAttempts' not in self._conf:
            self._conf['spark.yarn.maxAppAttempts'] = 1
        # reduce typos by providing common conf keys as kwargs
        if self._max_executors is not None:
            self._conf['spark.dynamicAllocation.maxExecutors'] = self._max_executors

        # Common deployment configuration for python tasks in discolytics
        if self._python:
            # Deploy venv if requested
            if isinstance(self._python, str):
                self._conf['spark.pyspark.python'] = 'venv/bin/python3.7'
                self._archives = append(self._archives, self._python + '#venv')
            elif not isinstance(self._python, bool):
                raise ValueError('Expected python kwargs as Optional[str|bool], found {}'.format(
                    type(self._python)))
            # tls to internal services, like relforge, only work if requests uses
            # the system ca certificates.
            self._env_vars['REQUESTS_CA_BUNDLE'] = '/etc/ssl/certs/ca-certificates.crt'
            # Make wmf_spark module available to all python executions
            self._py_files = append(self._py_files, REPO_PATH + '/spark/wmf_spark.py')
            # Inform wmf deployed spark-env.sh script what version
            # of python we intend to run, so it ships the correct
            # pyspark deps.
            self._spark_submit_env_vars['PYSPARK_PYTHON'] = 'python3.7'

        # SparkSubmitHook only applies env vars to the master, but we
        # want the executors too for consistency
        if self._env_vars:
            for k, v in self._env_vars.items():
                self._conf['spark.executorEnv.{}'.format(k)] = v

    def _make_hook(self):
        """
        Call the SparkSubmitHook to run the provided spark job
        """
        return SparkSubmitHook(
            # Sort for stable fixture output
            conf={k: v for k, v in sorted(self._conf.items())},
            conn_id=self._conn_id,
            files=self._files,
            py_files=self._py_files,
            archives=self._archives,
            driver_class_path=self._driver_class_path,
            jars=self._jars,
            java_class=self._java_class,
            packages=self._packages,
            exclude_packages=self._exclude_packages,
            repositories=self._repositories,
            total_executor_cores=self._total_executor_cores,
            executor_cores=self._executor_cores,
            executor_memory=self._executor_memory,
            driver_memory=self._driver_memory,
            keytab=self._keytab,
            principal=self._principal,
            proxy_user=self._proxy_user,
            name=self._name,
            num_executors=self._num_executors,
            application_args=self._application_args,
            env_vars=self._env_vars,
            verbose=self._verbose,
            spark_binary=self._spark_binary
        )

    def execute(self, context):
        if self._hook is None:
            self._hook = self._make_hook()
        env = os.environ.copy()
        if self._spark_submit_env_vars:
            env.update(self._spark_submit_env_vars)
        self._hook.submit(self._application, env=env)

    def on_kill(self):
        self._hook.on_kill()
