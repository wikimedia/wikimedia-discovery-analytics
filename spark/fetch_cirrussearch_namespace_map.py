"""
Fetch concrete namespace mapping from CirrusSearch

Queries the public mediawiki APIs to discover the per-wiki mapping
from namespace id to elasticsearch index. Overwrites an unpartitioned
table specified for output with the latest version of the map. The
table is overwritten as there is minimal value in historical/outdated
index mappings, and the lack of partitions simplifys downstream consumers
of this data having to decide what is the correct version.

This is integrated into hive and spark as the consumers of this data are in
hive and spark. Building out secondary integrations with, for example, bare
hdfs + json would add additional unnecessary complication for consumers.
"""
from argparse import ArgumentParser
import logging
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
import sys
from typing import Sequence, Tuple
from wmf_spark import HivePartition, HivePartitionWriter


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('--canonical-wikis-partition', required=True, type=HivePartition.from_spec)
    parser.add_argument('--output-partition', required=True, type=HivePartitionWriter.from_spec)
    return parser


def filter_wikiid_to_domain_name_map(df: DataFrame) -> DataFrame:
    """Filter to the set of active public wiki databases and their domain name"""
    return (
        df
        # We can only query live wikis
        .where(F.col('status') == 'open')
        # We can only query public wikis
        .where(F.col('visibility') == 'public')
        # login isn't a wiki in the traditional sense
        .where(F.col('database_group') != 'login')
        # test wikis related to horizon/cloud
        .where(F.col('database_group') != 'labtest')
        # new wikis can be added to the list before the domain name is known.
        # Typically empty string but could be null.
        .where(F.col('domain_name').isNotNull() & (F.col('domain_name') != ''))
        .select('database_code', 'domain_name')
    )


def establish_session():
    http = requests.Session()
    retries = Retry(total=5)
    http.mount('http://', HTTPAdapter(max_retries=retries))
    http.mount('https://', HTTPAdapter(max_retries=retries))
    return http


def fetch_namespaces(
    domain_name: str, session=establish_session()
) -> Sequence[Tuple[int, str]]:
    """Fetch the cirrus namespace map from wiki hosted at provided domain

    Because this is used as a pyspark UDF where there is not a convenient
    place to hold a session, the session is initialized when this function
    is defined and reused for all invocations.

    Returns a (namespace_id, elastic_index) tuple for all defined namespaces.
    """
    url = 'https://{}/w/api.php'.format(domain_name)
    response = session.get(url, timeout=5, params={
        'action': 'cirrus-config-dump',
        'format': 'json',
        'formatversion': 2,
    })
    response.raise_for_status()
    return [
        # The api made the keys into strings, but they are really ints
        (int(k), v)
        for k, v in response.json()['CirrusSearchConcreteNamespaceMap'].items()
    ]


def main(
    canonical_wikis_partition: HivePartition,
    output_partition: HivePartitionWriter
) -> int:
    spark = SparkSession.builder.getOrCreate()

    fetch_namespace_udf = F.udf(fetch_namespaces, T.ArrayType(T.StructType([
        T.StructField('namespace_id', T.IntegerType()),
        T.StructField('elastic_index', T.StringType()),
    ])))

    df = (
        filter_wikiid_to_domain_name_map(
            canonical_wikis_partition.read(spark))
        .withColumn('ns', F.explode(fetch_namespace_udf(F.col('domain_name'))))
        # The dataset is downright miniscule, perhaps 800 wikis and 50 namespaces
        # each. The whole thing should be done with a single partition.
        .coalesce(1)
        .select(
            F.col('database_code').alias('wikiid'),
            F.col('ns.namespace_id'),
            F.col('ns.elastic_index'))
    )

    # For simplicity of downstream use cases this job writes to an unpartitioned table. This
    # has the downside that if we attempt to overwrite the partition but the job fails the table
    # will be empty. Try and avoid some of this by calculating everything into memory before
    # issuing the write request
    df = df.cache()
    df.count()

    output_partition.overwrite_with(df)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
