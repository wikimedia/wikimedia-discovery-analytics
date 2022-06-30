from airflow.hooks.hive_hooks import HiveMetastoreHook


class HiveTablePath:
    """Returns the location on disk of a hive table.

    To use add as a filter in the dag:

        with DAG('example_dag', user_defined_filters={
            'hive_table_path': HiveTablePath(),
        }) as dag:

    The filter can then be applied to a string containg the
    fully qualified table name:

        {{ "my_db.my_table" | hive_table_path }}
    """
    def __init__(self, metastore_conn_id='metastore_default'):
        self.metastore_conn_id = metastore_conn_id
        self.hook = None

    def __call__(self, qualified_table):
        if qualified_table.startswith('hdfs://'):
            return qualified_table
        if self.hook is None:
            self.hook = HiveMetastoreHook(self.metastore_conn_id)
        if '.' not in qualified_table:
            raise ValueError('table must be fully qualified [{}]'.format(
                qualified_table))
        database_name, table_name = qualified_table.split('.', 2)
        with self.hook.metastore as client:
            table = client.get_table(database_name, table_name)
        return table.sd.location
