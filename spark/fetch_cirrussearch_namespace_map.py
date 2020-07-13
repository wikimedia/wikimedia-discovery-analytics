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
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
import sys
from typing import Optional, Sequence, Tuple


def load_wikiid_to_domain_name_map(spark: SparkSession, table_name: str) -> DataFrame:
    """Load the set of active public wiki databases and their domain name"""
    return (
        spark.read.table(table_name)
        # We can only query live wikis
        .where(F.col('status') == 'open')
        # We can only query public wikis
        .where(F.col('visibility') == 'public')
        # login isn't a wiki in the traditional sense
        .where(F.col('database_group') != 'login')
        .select('database_code', 'domain_name')
    )


def fetch_namespaces(
    domain_name: str, session=requests.Session()
) -> Sequence[Tuple[int, str]]:
    """Fetch the cirrus namespace map from wiki hosted at provided domain

    Because this is used as a pyspark UDF where there is not a convenient
    place to hold a session, the session is initialized when this function
    is defined and reused for all invocations.

    Returns a (namespace_id, elastic_index) tuple for all defined namespaces.
    """
    url = 'https://{}/w/api.php'.format(domain_name)
    response = session.get(url, params={
        'action': 'cirrus-config-dump',
        'format': 'json',
        'formatversion': 2,
    }).json()
    return [
        # The api made the keys into strings, but they are really ints
        (int(k), v)
        for k, v in response['CirrusSearchConcreteNamespaceMap'].items()
    ]


def main(raw_args: Optional[Sequence[str]] = None) -> int:
    parser = ArgumentParser()
    parser.add_argument('--canonical-wikis-table', required=True)
    parser.add_argument('--output-table', required=True)

    args = parser.parse_args(raw_args)

    spark = SparkSession.builder.getOrCreate()

    fetch_namespace_udf = F.udf(fetch_namespaces, T.ArrayType(T.StructType([
        T.StructField('namespace_id', T.IntegerType()),
        T.StructField('elastic_index', T.StringType()),
    ])))

    (
        load_wikiid_to_domain_name_map(
            spark, args.canonical_wikis_table)
        .withColumn('ns', F.explode(fetch_namespace_udf(F.col('domain_name'))))
        # The dataset is downright miniscule, perhaps 800 wikis and 50 namespaces
        # each. The whole thing should be done with a single partition.
        .coalesce(1)
        .registerTempTable('namespace_map_to_write')
    )

    spark.sql("""
        INSERT OVERWRITE TABLE {table}
        SELECT database_code, ns.namespace_id, ns.elastic_index
        FROM namespace_map_to_write
    """.format(
        table=args.output_table))

    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main(sys.argv[1:]))
