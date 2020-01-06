import logging
import os
import shlex
import time
from typing import Mapping, Optional

from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import skein
from skein.model import FinalStatus


class SkeinOperator(BaseOperator):
    template_fields = ('_application_args',)

    @apply_defaults
    def __init__(
        self,
        *args,
        application: str,
        application_args: Optional[str] = None,
        queue: str = 'default',
        memory: str = '1 GiB',
        vcores: int = 1,
        venv: Optional[str] = None,
        files: Optional[Mapping[str, str]] = None,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._application = application
        self._application_args = application_args
        self._queue = queue
        self._memory = memory
        self._vcores = vcores
        self._venv = venv
        self._files = files

    def _build_files(self) -> Mapping[str, str]:
        files = {
            os.path.basename(self._application): self._application
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

    def _build_script(self) -> str:
        """Build a bash script that will run our application on the executor"""
        if self._venv:
            python = os.path.join(self._venv_local_path(), 'bin/python')
        else:
            python = 'python3'

        args = [os.path.basename(self._application)]
        if self._application_args is not None:
            args += self._application_args
        arg_str = ' '.join(shlex.quote(arg) for arg in args)

        return python + ' ' + arg_str

    def _build_spec(self):
        return skein.ApplicationSpec(
            name=self.task_id,
            queue=self._queue,
            master=skein.Master(
                resources=skein.Resources(
                    memory=self._memory,
                    vcores=self._vcores,
                ),
                files=self._build_files(),
                script=self._build_script(),
            )
        )

    def execute(self, context):
        with skein.Client() as client:
            app_id = client.submit(self._build_spec())
            report = client.application_report(app_id)
            while report.final_status == "undefined":
                logging.info('Waiting for [%s] [%s]', report.id, report.state)
                time.sleep(30)
                report = client.application_report(app_id)
            if report.final_status != FinalStatus.SUCCEEDED:
                raise Exception('Failed running application')

    def on_kill(self):
        pass


# Defining the plugin class
class SkeinPlugin(AirflowPlugin):
    name = "skein_plugin"
    operators = [SkeinOperator]
