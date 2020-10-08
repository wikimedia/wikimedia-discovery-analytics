"""
Ingest pre-formatted files from hdfs into druid

Port of druid_loader.py and related oozie workflow from
analytics/refinery repository. This isn't too different
from DruidOperator provided by airflow, but it adds
some runtime checks and ingestion spec templating that
druid_loader.py was providing in oozie.
"""
import json
import os
import pwd
from typing import Mapping, Optional

from airflow.hooks.druid_hook import DruidHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from wmf_airflow.hdfs_cli import HdfsCliHook


def get_username() -> str:
    return pwd.getpwuid(os.getuid())[0]


class HdfsToDruidOperator(BaseOperator):
    template_fields = (
        'template_file', 'source_directory', 'loaded_period', 'target_datasource',
        'prod_username', 'hadoop_queue')

    @apply_defaults
    def __init__(
        self,
        # File path for the json template to use
        template_file: str,
        # Directory in hdfs that contains the data that should
        # be loaded in druid
        source_directory: str,
        # Time period (yyyy-MM-dd/yyyy-MM-dd) of the loaded data
        loaded_period: str,
        # The druid overload used for loading
        druid_ingest_conn_id: str = 'druid_ingest_default',
        # Datasource to be loaded in Druid. Can be empty if this
        # value is hard-coded in the loading template.
        target_datasource: Optional[str] = None,
        # Users other than the prod_username will have target_datasource
        # prepended with test_
        prod_username: Optional[str] = None,
        # TODO: What format?
        max_ingestion_time: Optional[str] = None,
        hadoop_queue: str = 'default',
        done_file: Optional[str] = '_SUCCESS',
        hook: Optional[DruidHook] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._hook = hook
        # General configuration that is typically the same between all invocations,
        # but can be changed as necessary.
        self.druid_ingest_conn_id = druid_ingest_conn_id
        self.hadoop_queue = hadoop_queue
        self.max_ingestion_time = max_ingestion_time
        self.prod_username = prod_username
        self.done_file = done_file

        # Job specific configuration
        self.template_file = template_file
        # Later use of urljoin() requires directory to be suffixed
        if not source_directory.endswith('/'):
            source_directory += '/'
        self.source_directory = source_directory
        self.loaded_period = loaded_period
        self.target_datasource = target_datasource

    def _check_user_datasource(self):
        if not HdfsCliHook.exists(self.source_directory):
            raise Exception('The output directory {} does not exist'.format(
                self.source_directory))
        if not HdfsCliHook.is_dir(self.source_directory):
            raise Exception('The given source_directory {} is not a directory'.format(
                self.source_directory))
        if self.done_file is not None:
            # urllib.parse.urljoin() is unusable here, it doesn't recognize hdfs://
            # as a valid protocol and simply returns the second arg.
            done_file = self.source_directory + self.done_file
            if not HdfsCliHook.exists(done_file):
                raise Exception(
                    'The job output directory {} lacks the {} marker: {}'.format(
                        self.source_directory, self.done_file, done_file))

    @property
    def safe_target_datasource(self) -> Optional[str]:
        if self.prod_username is None or self.prod_username == get_username():
            return self.target_datasource
        elif self.target_datasource is None:
            raise Exception(
                ('To prevent accidental data load only {} may '
                 'load arbitrary indices but you are {}').format(
                     self.prod_username, get_username()))
        elif self.target_datasource.startswith('test_'):
            return self.target_datasource
        else:
            return 'test_' + self.target_datasource

    def _apply_spec_templating(self, template: str) -> str:
        DRUID_DATASOURCE_FIELD = '*DRUID_DATASOURCE*'
        INTERVALS_ARRAY_FIELD = '*INTERVALS_ARRAY*'
        INPUT_PATH_FIELD = '*INPUT_PATH*'
        HADOOP_QUEUE_FIELD = '*HADOOP_QUEUE*'

        intervals_array = '["{0}"]'.format(self.loaded_period)
        spec = (template.
                replace(INPUT_PATH_FIELD, self.source_directory).
                replace(INTERVALS_ARRAY_FIELD, intervals_array).
                replace(HADOOP_QUEUE_FIELD, self.hadoop_queue))

        datasource = self.safe_target_datasource
        if datasource is not None:
            spec = spec.replace(DRUID_DATASOURCE_FIELD, datasource)
        elif DRUID_DATASOURCE_FIELD in spec:
            raise Exception('Template expects a druid datasource but none was provided')
        return spec

    @property
    def index_spec(self) -> Mapping:
        with open(self.template_file, 'r') as f:
            index_spec_template = f.read()
        index_spec_str = self._apply_spec_templating(index_spec_template)
        self.log.info('Templated spec:\n%s', index_spec_str)
        return json.loads(index_spec_str)

    def execute(self, context):
        self._check_user_datasource()
        if self._hook is None:
            self._hook = DruidHook(
                druid_ingest_conn_id=self.druid_ingest_conn_id,
                max_ingestion_time=self.max_ingestion_time)
        self._hook.submit_indexing_job(self.index_spec)


class HdfsToDruidPlugin(AirflowPlugin):
    name = "hdfs_to_druid_plugin"
    operators = [HdfsToDruidOperator]
