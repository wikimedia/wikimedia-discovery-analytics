from datetime import timedelta
from typing import Optional, Union

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from wmf_airflow.skein import SkeinHook


class SwiftUploadOperator(BaseOperator):
    template_fields = (
        '_swift_container', '_source_directory', '_swift_object_prefix',
        '_application', '_swift_auth_file', '_event_service_url',
        '_swift_delete_after', '_event_stream')

    @apply_defaults
    def __init__(
        self,
        swift_container: str,
        source_directory: str,
        swift_object_prefix: str,
        swift_upload_py: str = (
            'hdfs://analytics-hadoop/wmf/refinery/current/'
            'oozie/util/swift/upload/swift_upload.py'),
        swift_auth_file: str = (
            'hdfs://analytics-hadoop/user/analytics/'
            'swift_auth_analytics_admin.env'),
        event_stream: Union[bool, str] = True,
        swift_overwrite: bool = False,
        swift_delete_after: Union[str, timedelta] = timedelta(days=30),
        swift_auto_version: bool = False,
        event_per_object: bool = False,
        event_service_url: str = 'https://eventgate-analytics.svc.eqiad.wmnet:4592/v1/events',
        name: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._application = swift_upload_py
        self._swift_auth_file = swift_auth_file
        self._swift_container = swift_container
        self._source_directory = source_directory
        self._swift_object_prefix = swift_object_prefix
        if event_stream is True:
            self._event_stream = 'swift.{}.upload-complete'.format(swift_container)
        elif event_stream is False:
            self._event_stream = 'false'
        else:
            assert isinstance(event_stream, str)
            self._event_stream = event_stream
        self._swift_overwrite = swift_overwrite
        self._swift_delete_after = swift_delete_after
        self._swift_auto_version = swift_auto_version
        self._event_per_object = event_per_object
        self._event_service_url = event_service_url
        self._name = name
        self._hook = None

    @property
    def _swift_delete_after_sec(self) -> int:
        try:
            return int(self._swift_delete_after.total_seconds())  # type: ignore
        except AttributeError:
            # Templating can only return strings, interperet as seconds.
            # macros.timedelta(...).total_seconds() can be used with more
            # human values. We cast to float instead of int as macros
            # may emit them, and int doesn't accept float strings.
            return int(float(self._swift_delete_after))  # type: ignore

    def _make_hook(self):
        return SkeinHook(
            name=self.task_id if self._name is None else self._name,
            files={
                'swift_auth.env': self._swift_auth_file
            },
            application_args=[
                '--swift-overwrite', str(self._swift_overwrite).lower(),
                '--swift-delete-after', str(self._swift_delete_after_sec),
                '--swift-auto-version', str(self._swift_auto_version).lower(),
                '--swift-object-prefix', self._swift_object_prefix,
                '--event-per-object', str(self._event_per_object).lower(),
                '--event-stream', self._event_stream,
                '--event-service-url', self._event_service_url,
                'swift_auth.env',
                self._swift_container,
                self._source_directory
            ])

    def execute(self, context):
        if self._hook is None:
            self._hook = self._make_hook()
        self._hook.submit(self._application)
