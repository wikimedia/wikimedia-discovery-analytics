import subprocess

from airflow.operators.sensors import BaseSensorOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults


class HdfsCliHook(BaseHook):
    # TODO: Connection support. For now, simply interact
    # with hdfs command and it's system defaults.

    @staticmethod
    def _test(flag, path):
        status_code = subprocess.call([
            'hdfs', 'dfs', '-test', flag, path])
        return status_code == 0

    @staticmethod
    def exists(path: str) -> bool:
        return HdfsCliHook._test('-e', path)

    @staticmethod
    def is_dir(path: str) -> bool:
        return HdfsCliHook._test('-d', path)

    @staticmethod
    def is_file(path: str) -> bool:
        return HdfsCliHook._test('-f', path)

    @staticmethod
    def rm(path: str, recurse=False, force=False) -> bool:
        cmd = ['hdfs', 'dfs', '-rm']
        if recurse:
            cmd.append('-r')
        if force:
            cmd.append('-f')
        cmd.append(path)
        status_code = subprocess.call(cmd)
        return status_code == 0

    @staticmethod
    def mkdir(path: str, parents=False) -> bool:
        cmd = ['hdfs', 'dfs', '-mkdir']
        if parents:
            cmd.append('-p')
        cmd.append(path)
        status_code = subprocess.call(cmd)
        return status_code == 0

    @staticmethod
    def text(path: str, encoding: str = 'utf8') -> str:
        try:
            raw_bytes = subprocess.check_output([
                'hdfs', 'dfs', '-text', path])
        except subprocess.CalledProcessError:
            raise FileNotFoundError(path)
        return raw_bytes.decode(encoding)


class HdfsCliSensor(BaseSensorOperator):
    template_fields = ('filepath',)

    @apply_defaults
    def __init__(self,
                 filepath: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filepath = filepath

    def poke(self, context):
        self.log.info('Checking marker at {}'.format(self.filepath))
        return HdfsCliHook.exists(self.filepath)
