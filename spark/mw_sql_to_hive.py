"""
Export an SQL query on all mediawiki databases to a hive table

This script requires dnspython to be installed to resolve
host/port for mariadb replicas

"""
from argparse import ArgumentParser
from configparser import ConfigParser
from datetime import datetime
import logging
import sys
from typing import Mapping, Sequence, Tuple

import dns.resolver
from pyspark.sql import SparkSession, DataFrame, functions as F


# These wikis dont seem to load properly, and are very special case wikis
EXCLUDE_WIKIS = {'labswiki', 'labtestwiki'}


def arg_parser() -> ArgumentParser:
    def date(val: str) -> datetime:
        return datetime.strptime(val, '%Y-%m-%d')

    def csv(val: str) -> Sequence[str]:
        return val.split(',')

    parser = ArgumentParser()
    parser.add_argument(
        '--mysql-defaults-file', default='/etc/mysql/conf.d/analytics-research-client.cnf')
    parser.add_argument(
        '--dblists', required=True, type=csv,
        help='csv of mediawiki-config s*.dblist files to source wiki to shard mapping from')
    parser.add_argument(
        '--date', required=True, type=date,
        help='Date to use for output table partition specification')
    parser.add_argument(
        '--query', required=True,
        help='SQL query to run against analytics replicas')
    parser.add_argument(
        '--output-table', required=True,
        help='Hive table to write query results to')
    parser.add_argument(
        '--num-output-partitions', type=int, default=20,
        help='Number of partitions to write to hdfs. Estimate value based on 100MB per partition')
    return parser


def _get_mediawiki_section_dbname_mapping(
    dblist_section_paths: Sequence[Tuple[str, Sequence[str]]]
) -> Mapping[str, str]:
    """Parse mapping from wiki dbname to database shard from dblists

    Found in operations/mediawiki-config repository at /dblists/s*.dblist
    """
    db_mapping = {}
    for dblist_section_path, lines in dblist_section_paths:
        shard = dblist_section_path.strip() \
            .rstrip('.dblist').split('/')[-1]
        for db in lines:
            if '#' in db:
                db = db[:db.find('#')]
            db = db.strip()
            if db:
                db_mapping[db] = shard
    return db_mapping


def get_mediawiki_section_dbname_mapping(
    dblist_section_paths: Sequence[str],
) -> Mapping[str, str]:
    resolved = []
    for path in dblist_section_paths:
        with open(path, 'rt') as f:
            resolved.append((path, f.readlines()))
    return _get_mediawiki_section_dbname_mapping(resolved)


def memoize(wrapped):
    state = {}

    def fn(*args):
        if args not in state:
            state[args] = wrapped(*args)
        return state[args]
    return fn


@memoize
def get_dbstore_host_port(shard: str) -> Tuple[str, int]:
    """Determine host/port for wiki mysql shard

    We have special DNS SRV records to map from a shard name
    to the appropriate analytics replicas.
    See https://wikitech.wikimedia.org/wiki/Analytics/Systems/MariaDB
    """
    answers = dns.resolver.query('_' + shard + '-analytics._tcp.eqiad.wmnet', 'SRV')
    host, port = str(answers[0].target), answers[0].port
    return (host, port)


def get_mysql_options_file_user_pass(content: str) -> Tuple[str, str]:
    """Parse username and password from mysql defaults file"""
    # ConfigParser doesn't like any % in the password and wants them doubled up...
    content = content.replace('%', '%%')
    parser = ConfigParser()
    parser.read_string(content)
    return parser.get('client', 'user'), parser.get('client', 'password')


class WikiDbQuery:
    def __init__(
        self,
        spark: SparkSession,
        dbname_mapping: Mapping[str, str],
        user: str,
        password: str
    ):
        self.spark = spark
        self.dbname_mapping = dbname_mapping
        self.user = user
        self.password = password

    def _reader(self, dbname):
        shard = self.dbname_mapping[dbname]
        host, port = get_dbstore_host_port(shard)
        return (
            self.spark.read.format('jdbc')
            .option('url', 'jdbc:mysql://{}:{}/{}'.format(host, port, dbname))
            # For whatever reason the driver is not found auto-magically by name
            .option('driver', 'com.mysql.cj.jdbc.Driver')
            # Modern mysql connector defaults to ssl, but analytics replicas don't have it
            .option('useSSL', 'false')
            # Authentication
            .option('user', self.user)
            .option('password', self.password)
        )

    def query(self, dbname, query):
        return (
            self._reader(dbname)
            .option('query', query)
            .load()
            .withColumn('wikiid', F.lit(dbname))
        )


def union_all_df(dfs: Sequence[DataFrame]) -> DataFrame:
    """Union together any number of DataFrames

    All provided dataframes must have identical schemas.
    """
    if len(dfs) == 1:
        return dfs[0]
    elif len(dfs) == 0:
        raise ValueError('No DataFrames provided!')
    else:
        # Unioning dataframes only works with 2 at a time, leading to a *very* deep
        # graph when doing hundreds of wikis.  Instead convert everything to RDD
        # where we can union in a single step of the computation graph. This
        # is still slow due to df->rdd->df conversion, but is highly parallelizable
        # as opposed to 900 sequential union's.
        spark = dfs[0].sql_ctx.sparkSession
        return spark.createDataFrame(
            spark.sparkContext.union([df.rdd for df in dfs]),  # type: ignore
            dfs[0].schema)


def write_partition(
    df: DataFrame,
    output_table: str,
    partition_spec: Mapping[str, str],
) -> None:
    for k, v in partition_spec.items():
        df = df.withColumn(k, F.lit(v))
    expect_schema = df.sql_ctx.read.table(output_table).schema
    if set(df.schema.names) != set(expect_schema.names):
        raise ValueError('Schemas do not have matching names: {} != {}'.format(df.schema.names, expect_schema.names))

    df.createTempView('tmp_output_df')
    insert_stmt = """
        INSERT OVERWRITE TABLE {table}
        PARTITION({partition_stmt})
        SELECT {select_columns}
        FROM tmp_output_df
    """.format(
        table=output_table,
        partition_stmt=','.join('{}={}'.format(k, v) for k, v in partition_spec.items()),
        # choose field names from expected table schema
        # to ensure same ordering
        select_columns=','.join(field for field in expect_schema.names if field not in partition_spec))

    df.sql_ctx.sql(insert_stmt)


def main(
    mysql_defaults_file: str,
    dblists: Sequence[str],
    date: datetime,
    query: str,
    output_table: str,
    num_output_partitions: int
) -> int:
    dbname_mapping = get_mediawiki_section_dbname_mapping(dblists)
    with open(mysql_defaults_file, 'rt') as f:
        user, password = get_mysql_options_file_user_pass(f.read())
    spark = SparkSession.builder.getOrCreate()
    loader = WikiDbQuery(spark, dbname_mapping, user, password)

    wikis = [dbname for dbname in dbname_mapping.keys() if dbname not in EXCLUDE_WIKIS]
    per_wiki_dfs = [loader.query(dbname, query) for dbname in wikis]

    # If we don't repartition the output we will have 1 per source database, most
    # of those will be tiny wikis with very few rows, and then a few giants like
    # enwiki will introduce significant skew. Instead randomly repartition into
    # even pieces.
    df_out = union_all_df(per_wiki_dfs) \
        .repartition(num_output_partitions)

    write_partition(
        df=df_out,
        output_table=output_table,
        partition_spec={
            'year': str(date.year),
            'month': str(date.month),
            'day': str(date.day),
        })

    return 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
