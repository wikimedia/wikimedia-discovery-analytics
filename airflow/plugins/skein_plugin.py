import logging
import os
import shlex
import time
from typing import Mapping, Optional

from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import skein
from skein.model import FinalStatus


class SkeinHook(BaseHook):
    def __init__(
        self,
        conn_id='skein_default',
        name='default-name',
        application_args: Optional[str] = None,
        queue: str = 'default',
        memory: str = '1 GiB',
        vcores: int = 1,
        venv: Optional[str] = None,
        files: Optional[Mapping[str, str]] = None,
    ):
        self._conn_id = conn_id
        self._name = name
        self._application_args = application_args
        self._queue = queue
        self._memory = memory
        self._vcores = vcores
        self._venv = venv
        self._files = files

    def _build_files(self, application: str) -> Mapping[str, str]:
        files = {
            os.path.basename(application): application
        }
        if self._venv is not None:
            files[os.path.basename(self._venv)] = self._venv
        if self._files:
            files.update(self._files)
        return files

    def _venv_local_path(self) -> str:
        assert self._venv is not None
        """Path inside venv on executor"""
        basename = os.path.basename(self._venv)
        if basename is None:
            raise Exception('Could not detect basename from venv: {}'.format(self._venv))
        for ext in ('.zip', '.tgz', '.tar.gz'):
            if basename.lower().endswith(ext):
                return basename[:-len(ext)]
        raise Exception('Unrecognized virtualenv extension: {}'.format(basename))

    def _build_script(self, application: str) -> str:
        """Build a bash script that will run our application on the executor"""
        if self._venv:
            python = os.path.join(self._venv_local_path(), 'bin/python')
        else:
            python = 'python3'

        args = [os.path.basename(application)]
        if self._application_args is not None:
            args += self._application_args
        arg_str = ' '.join(shlex.quote(arg) for arg in args)

        return python + ' ' + arg_str

    def _build_spec(self, application: str):
        return skein.ApplicationSpec(
            name=self._name,
            queue=self._queue,
            master=skein.Master(
                resources=skein.Resources(
                    memory=self._memory,
                    vcores=self._vcores,
                ),
                files=self._build_files(application),
                script=self._build_script(application),
            )
        )

    def submit(self, application):
        # If the client ever needs configuration it should
        # be done through self._conn_id
        with skein.Client() as client:
            app_id = client.submit(self._build_spec(application))
            report = client.application_report(app_id)
            while report.final_status == "undefined":
                logging.info('Waiting for [%s] [%s]', report.id, report.state)
                time.sleep(30)
                report = client.application_report(app_id)
            if report.final_status != FinalStatus.SUCCEEDED:
                raise Exception('Failed running application')

    def on_kill(self):
        pass


class SkeinOperator(BaseOperator):
    template_fields = ('_application_args',)

    @apply_defaults
    def __init__(
        self,
        application: str,
        application_args: Optional[str] = None,
        conn_id: str = 'skein_default',
        queue: str = 'default',
        memory: str = '1 GiB',
        vcores: int = 1,
        venv: Optional[str] = None,
        files: Optional[Mapping[str, str]] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._application = application
        self._application_args = application_args
        self._conn_id = conn_id
        self._queue = queue
        self._memory = memory
        self._vcores = vcores
        self._venv = venv
        self._files = files
        self._hook = None

    def execute(self, context):
        if self._hook is None:
            self._hook = SkeinHook(
                conn_id=self._conn_id,
                name=self.task_id,
                application_args=self._application_args,
                queue=self._queue,
                memory=self._memory,
                vcores=self.vcores,
                venv=self.venv,
                files=self.files)
        self._hook.submit(self._application)

    def on_kill(self):
        if self._hook:
            self._hook.on_kill()


# Defining the plugin class
class SkeinPlugin(AirflowPlugin):
    name = "skein_plugin"
    hooks = [SkeinHook]
    operators = [SkeinOperator]
