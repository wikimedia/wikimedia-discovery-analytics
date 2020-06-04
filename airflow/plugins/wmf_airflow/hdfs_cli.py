import subprocess

from airflow.hooks.base_hook import BaseHook


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
    def text(path: str, encoding: str = 'utf8') -> str:
        try:
            raw_bytes = subprocess.check_output([
                'hdfs', 'dfs', '-text', path])
        except subprocess.CalledProcessError:
            raise FileNotFoundError(path)
        return raw_bytes.decode(encoding)
