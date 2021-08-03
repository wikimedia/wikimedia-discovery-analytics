import os

from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


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
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._application = application
        self._conf = conf
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
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._keytab = keytab
        self._principal = principal
        self._proxy_user = proxy_user
        self._name = name
        self._num_executors = num_executors
        self._application_args = application_args
        self._env_vars = env_vars
        self._spark_submit_env_vars = spark_submit_env_vars
        self._verbose = verbose
        self._spark_binary = spark_binary
        self._hook = None
        self._conn_id = conn_id

    def _make_hook(self):
        """
        Call the SparkSubmitHook to run the provided spark job
        """
        # SparkSubmitHook only applies env vars to the master, but we
        # want the executors too for consistency
        conf = self._conf.copy() if self._conf else {}
        if self._env_vars:
            for k, v in self._env_vars.items():
                conf['spark.executorEnv.{}'.format(k)] = v

        return SparkSubmitHook(
            # Sort for stable fixture output
            conf={k: v for k, v in sorted(conf.items())},
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
