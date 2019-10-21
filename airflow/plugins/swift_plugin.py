from datetime import timedelta
from typing import List, Optional, Union

from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperator
from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
from airflow.utils.decorators import apply_defaults


class SwiftUploadOperator(BaseOperator):
    template_params = ('_upload_user', '_source_directory')

    @apply_defaults
    def __init__(
        self,
        swift_upload_py: str,
        swift_auth_env: str,
        swift_container: str,
        source_directory: str,
        swift_object_prefix: Optional[str] = None,
        conn_id: str = 'spark_default',
        event_stream: Union[bool, str] = True,
        swift_overwrite: bool = False,
        swift_delete_after: timedelta = timedelta(days=30),
        swift_auto_version: bool = False,
        event_per_object: bool = False,
        event_service_url: str = 'http://eventgate-analytics.svc.eqiad.wmnet:31192/v1/events',
        upload_user: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._swift_upload_py = swift_upload_py
        self._conn_id = conn_id
        self._swift_auth_env = swift_auth_env
        self._swift_container = swift_container
        self._source_directory = source_directory
        self._swift_object_prefix = swift_object_prefix or self.task_id
        self._event_stream = event_stream
        self._swift_overwrite = swift_overwrite
        self._swift_delete_after = swift_delete_after
        self._swift_auto_version = swift_auto_version
        self._event_per_object = event_per_object
        self._event_service_url = event_service_url
        self._upload_user = upload_user or self.owner

    def _validate(self) -> List[str]:
        # Late import allows for cross-plugin dependencies
        from airflow.hooks.hdfs_cli_plugin import HdfsCliHook
        errors = []

        if not HdfsCliHook.exists(self._source_directory):
            errors.append('...')
        elif not HdfsCliHook.is_dir(self._source_directory):
            errors.append('...')

        if not HdfsCliHook.exists(self._auth_file):
            errors.append('...')

        return errors

    # Let test cases replace this, due to how airflow imports
    # things they can't simply mock objects in our module.
    _make_spark_hook = SparkSubmitHook

    def execute(self, context):
        # Rather than deal with hdfs and getting things into place ourselves,
        # spin up a spark driver and let it act like a python runner with
        # yarn and hdfs integration.
        if self._event_stream is True:
            event_stream = 'swift.{}.upload-complete'.format(self._swift_container)
        elif self._event_stream is False:
            event_stream = 'false'
        else:
            event_stream = self._event_stream

        self._hook = self._make_spark_hook(
            conn_id=self._conn_id,
            files='{}#swift_auth.env'.format(self._swift_auth_env),
            application_args=[
                '--upload-user', self._upload_user,
                '--swift-overwrite', str(self._swift_overwrite).lower(),
                '--swift-delete-after', str(int(self._swift_delete_after.total_seconds())),
                '--swift-auto-version', str(self._swift_auto_version).lower(),
                '--swift-object-prefix', self._swift_object_prefix,
                '--event-per-object', str(self._event_per_object).lower(),
                '--event-stream', event_stream,
                '--event-service-url', self._event_service_url,
                'swift_auth.env',
                self._swift_container,
                self._source_directory
            ])
        self._hook.submit(self._swift_upload_py)

    def on_kill(self):
        self._hook.on_kill()


# Defining the plugin class
class SwiftPlugin(AirflowPlugin):
    name = "swift_plugin"
    operators = [SwiftUploadOperator]
