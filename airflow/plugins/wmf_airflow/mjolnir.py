from collections import OrderedDict
import json
import os
from typing import cast, Any, Dict, List, Mapping, Optional, Tuple, TypeVar, Union

from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.log.logging_mixin import LoggingMixin

from wmf_airflow.hdfs_cli import HdfsCliHook

_T = TypeVar('_T')


KNOWN_TRANSFORMERS = {
    'dbn', 'hyperparam', 'norm_query_clustering',
    'train', 'feature_selection', 'make_folds',
    'feature_vectors', 'query_clicks_ltr',
}

# Executor auto-sizing
AUTO_DETECT_CONFIG = {
    # No more than ~1/4 of cluster memory
    'agg_memory_limit': '1T',
    # No more than ~1/4 of cluster cores
    'agg_cores_limit': 450,
    # max memory that can be assigned to single executor
    'executor_max_memory': '50G',
    # Amount of memory needed for pyspark/jvm
    'baseline_memory_overhead': '512M',
    # Number of bytes of memory overhead to allocate per
    # value of the feature matrix
    'bytes_per_value': {
        'make_folds': 30,
        'hyperparam': 30,
        'train': 20
    }
}


def _merge_spark_args(default: Mapping, override: Mapping) -> Dict:
    output = dict(default, **override)
    for key, merger in {
        'conf': lambda a, b: dict(a, **b),
        'packages': lambda a, b: a + ',' + b,
        'jars': lambda a, b: a + ',' + b,
    }.items():
        if key in default and key in override:
            output[key] = merger(default[key], override[key])
    return output


class AutoSizeSpark(LoggingMixin):
    def __init__(
        self,
        transformer: str,
        wiki: Optional[str],
        metadata_dir: Optional[str],
        config: Mapping = AUTO_DETECT_CONFIG
    ):
        self._transformer = transformer
        self._wiki = wiki
        self._config = config
        self._metadata_dir = metadata_dir

    @staticmethod
    def _parse_memory_to_mb(memory: Union[int, str]) -> int:
        """Parse spark style memory specification.

        Primarily a helper for reading and adjusting spark configuration.
        """
        if isinstance(memory, int):
            return memory
        elif not isinstance(memory, str):
            raise Exception('Expected integer or string memory value')
        if memory.isdigit():
            return int(memory)

        suffixes = {
            'T': 2 ** 20,
            'G': 2 ** 10,
            'M': 1,
        }
        for suffix, scale in suffixes.items():
            if memory[-1].upper() == suffix:
                return int(memory[:-1]) * scale
        raise Exception('Unrecognized memory spec: {}'.format(memory))

    def _read_metadata(self):
        text_raw = HdfsCliHook.text(os.path.join(
            self._metadata_dir, '_METADATA.JSON'))
        return json.loads(text_raw)

    def _detect_memory_overhead_mb(self, dim: Tuple[int, int]) -> int:
        """Heuristic to decide the amount of memory overhead necessary"""
        num_obs, num_features = dim
        bytes_per_value = self._config['bytes_per_value'][self._transformer]
        bytes_estimate = num_obs * (num_features + 1) * bytes_per_value
        matrix_overhead_mb = bytes_estimate / 2**20

        baseline_memory_overhead_mb = self._parse_memory_to_mb(
            self._config['baseline_memory_overhead'])
        return int(baseline_memory_overhead_mb + matrix_overhead_mb)

    def _limit_max_executors(self, spark_conf: Mapping) -> int:
        """Determine the the maximum number of executors within provided limits"""
        executor_memory_mb = self._parse_memory_to_mb(spark_conf['spark.executor.memory']) \
            + self._parse_memory_to_mb(spark_conf.get('spark.executor.memoryOverhead', '256m'))
        memory_limit_mb = self._parse_memory_to_mb(self._config['agg_memory_limit'])

        executor_cores = int(spark_conf.get('spark.executor.cores', 1))
        cores_limit = self._config['agg_cores_limit']

        by_memory = memory_limit_mb // executor_memory_mb
        by_cores = cores_limit // executor_cores
        return min(by_memory, by_cores)

    def _detect_dimensions(self, metadata: Mapping) -> Tuple[(int, int)]:
        if self._transformer == 'make_folds':
            assert self._wiki is not None
            num_obs = metadata['num_obs'][self._wiki]
            num_feat = len(metadata['wiki_features'][self._wiki])
        else:
            assert self._wiki is None
            num_obs = metadata['metadata']['num_obs']
            num_feat = len(metadata['metadata']['features'])
        return (num_obs, num_feat)

    def apply(self, spark_conf: Mapping) -> Mapping:
        spark_conf = dict(spark_conf)
        if self._transformer in self._config['bytes_per_value']:
            metadata = self._read_metadata()
            dim = self._detect_dimensions(metadata)
            spark_conf['spark.executor.memoryOverhead'] = \
                self._detect_memory_overhead_mb(dim)
            self.log.info(
                'Transformer [{}] with dims [{}] detected memory overhead of [{}] mb'.format(
                    self._transformer, dim,
                    spark_conf['spark.executor.memoryOverhead']))

        spark_conf['spark.dynamicAllocation.maxExecutors'] = \
            self._limit_max_executors(spark_conf)
        self.log.info('Detected max executors of {}'.format(
            spark_conf['spark.dynamicAllocation.maxExecutors']))
        return spark_conf


def _sort_items_recursive(maybe_dict):
    """Recursively sort dictionaries so iteration gives deterministic outputs"""
    if hasattr(maybe_dict, 'items'):
        items = ((k, _sort_items_recursive(v)) for k, v in maybe_dict.items())
        return OrderedDict(sorted(items, key=lambda x: x[0]))
    else:
        return maybe_dict


def hive_partition_path(table: str, partition_spec: List[Tuple[str, str]]) -> str:
    if table.startswith('hdfs://'):
        table_expr = table
    else:
        # We don't want to lookup paths from hive when instantiating the DAG,
        # so we delay those until tasks are executed. This requires a special
        # template filter, hive_table_path, that can only be provided by
        # the DAG and not plugins.
        table_expr = '{{ "' + table + '" | hive_table_path }}'
    part_expr = ('{}={}'.format(k, v) for k, v in partition_spec)
    return os.path.join(table_expr, *part_expr)


class MjolnirOperator(BaseOperator, LoggingMixin):
    """Run spark/mjolnir scripts

    Not the cleanest implementation, but tries to keep repetative
    declarations out of the definitions.
    """
    template_fields = (
        '_table', '_partition_spec', '_transformer_args',
        '_output_path', '_auto_size_metadata_dir', '_deploys')

    @apply_defaults
    def __init__(
        self,
        table: str,
        partition_spec: List[Tuple[str, str]],
        deploys: Mapping,
        output_path: Optional[str] = None,
        marker: str = '_SUCCESS',
        transformer: Optional[str] = None,
        transformer_args: Mapping[str, Any] = {},
        spark_args: Mapping[str, Any] = {},
        metastore_conn_id: str = 'metastore_default',
        auto_size_metadata_dir: Optional[str] = None,
        python_version: str = 'python3.7',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._transformer = transformer or self.task_id
        if self._transformer not in KNOWN_TRANSFORMERS:
            raise ValueError('Unknown transformer [{}]'.format(self._transformer))
        self._transformer_args = transformer_args
        self._deploys = deploys
        self._spark_args = dict(spark_args)
        self._table = table
        self._partition_spec = list(partition_spec)
        if output_path is None:
            self._output_path = hive_partition_path(table, self._partition_spec)
        else:
            self._output_path = output_path
        self._marker = marker
        self._metastore_conn_id = metastore_conn_id
        self._auto_size_metadata_dir = auto_size_metadata_dir
        self._python_version = python_version

    def partition_key(self, key: str):
        """Report partitioning information about this operations output"""
        return dict(self._partition_spec)[key]

    def _marker_exists(self):
        """Check if the 'operation complete' marker file exists"""
        marker_path = os.path.join(self._output_path, self._marker)
        self.log.info('Checking marker at {}'.format(marker_path))
        return HdfsCliHook.exists(marker_path)

    def _application_args(self, context: Mapping, output_path: str) -> List[str]:
        """Construct args to be passed to mjolnir python script"""
        application_args = [
            # Start with the transformer to invoke
            self._transformer,
            # Arg added by default to all scripts
            '--date', context['ds_nodash'],
            # Arg added by convention to all scripts
            '--output-path', output_path,
        ]
        # Sort for deterministic output
        for k, v in sorted(self._transformer_args.items(), key=lambda x: x[0]):
            application_args.append('--' + k)
            # Accepting arrays handles multi-arg, such as: --wiki eswiki ruwiki
            if not isinstance(v, list):
                v = [v]
            for arg in v:
                if arg is None:
                    raise TypeError('None is not a valid cli arg')
                application_args.append(str(arg))
        return application_args

    def _default_spark_args(self) -> Dict[str, Any]:
        args = cast(Dict[str, Any], dict())
        # Ship a venv with all the mjolnir code to executors
        args['archives'] = '{}#mjolnir_venv'.format(self._deploys['mjolnir_venv'])
        args['driver_memory'] = '2g'
        # mjolnir jvm helpers
        args['packages'] = 'org.wikimedia.search:mjolnir:0.5-SNAPSHOT'
        # Pull in analytics/refinery-hive for some UDF's
        args['jars'] = os.path.join(
            self._deploys['refinery'], 'artifacts/refinery-hive.jar')

        conf = cast(Dict[str, Any], dict())
        # Retry at the airflow level instead of in spark
        conf['spark.yarn.maxAppAttempts'] = '1'
        # Use the venv shipped in archives
        conf['spark.pyspark.python'] = 'mjolnir_venv/bin/python'
        # Fetch jars from archiva. This must be a local file,
        # it cannot be an hdfs path.
        conf['spark.jars.ivySettings'] = '/etc/maven/ivysettings.xml'
        # By default ivy will use $HOME/.ivy2, but the airflow user
        # has no home directory. Use /tmp for now...
        conf['spark.jars.ivy'] = '/tmp/airflow_ivy2'
        # Default resources limits
        conf['spark.executor.memory'] = '2g'
        conf['spark.dynamicAllocation.maxExecutors'] = '600'
        conf['spark.sql.shuffle.partitions'] = '1000'
        args['conf'] = conf

        return args

    def execute(self, context: Mapping):
        self.log.info('Using output path of {}'.format(self._output_path))
        if self._marker_exists():
            # To re-run the outputs must be deleted.
            self.log.info('Output marker exists, skipping task.')
        else:
            self._execute(context)

    def _execute(self, context: Mapping):
        application_args = self._application_args(context, self._output_path)
        spark_args = self._default_spark_args()
        if self._spark_args:
            spark_args = _merge_spark_args(spark_args, self._spark_args)

        auto_size = AutoSizeSpark(
            self._transformer, self._transformer_args.get('wiki'),
            self._auto_size_metadata_dir)
        spark_args['conf'] = auto_size.apply(spark_args['conf'])

        # Sort dicts to make output deterministic. The top level sort doesn't
        # matter, but the inner dict sorts ensure dicts like spark_args['conf']
        # are always output in a consistent order.
        spark_args = _sort_items_recursive(spark_args)

        self._hook = SparkSubmitHook(
            name='mjolnir-{}-{}-spark'.format(
                self.task_id, context['ds_nodash']),
            application_args=application_args,
            **spark_args)

        # spark-env.sh shipped to wmf analytics will incorrectly detect the
        # system python version and adjust PYTHONPATH, even though we are using
        # deploy-mode=cluster which never uses the system python on the airflow
        # instance. Work around by telling spark-env.sh, through PYSPARK_PYTHON,
        # the version we will be using. Spark will override this env var with the
        # spark.pyspark.python configuration supplied with the cli args.
        env = os.environ.copy()
        if 'PYSPARK_PYTHON' not in env:
            env['PYSPARK_PYTHON'] = self._python_version

        # Dummy script that imports and runs mjolnir.__main__.main
        application_path = os.path.join(
            self._deploys['discovery-analytics'], 'spark/mjolnir-utilities.py')
        self._hook.submit(application_path, env=env)

    def on_kill(self):
        self._hook.on_kill()
