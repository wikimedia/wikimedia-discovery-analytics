# A very primitive script to transfer from hive data files
# to elasticsearch bulk import formatted files on swift.
from __future__ import print_function
try:
    from pyspark.sql import SparkSession, functions as F
except ImportError:
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession, functions as F
from argparse import ArgumentParser
import json
import math
import random


def documentData(document, field):
    """Transform document into elasticsearch bulk import lines"""
    header = {
        'update': {
            '_index': '{}_content'.format(document.wikiid),
            '_type': 'page',
            '_id': document.page_id,
        }
    }

    update = {
        'script': {
            'source': 'super_detect_noop',
            'lang': 'super_detect_noop',
            'params': {
                'handlers': {
                    field: 'within 20%'
                },
                'source': {
                    field: document.score
                }
            }
        }
    }

    return '{}\n{}'.format(json.dumps(header), json.dumps(update))


def unique_value_per_partition(df, limit_per_partition, col_name):
    """Force each partition to contain only one value for `col_name`

    Handles skew by looping over the dataset twice, first to count and a second
    time to distribute rows between N partitions per distinct value.

    Each output file will be imported by a single python process. Individual
    imports should be sized to take no more than 15 minutes to import.
    Additionally individual files should be for a single index to allow
    writes to all go to the same segments. Achieve this by defining an
    explicit partitioning scheme with a number of partitions per wiki.
    """
    start = 0
    partition_ranges = {}
    for row in df.groupBy(df[col_name]).count().collect():
        end = int(start + math.ceil(row['count'] / limit_per_partition))
        # -1 is because randint is inclusive, and end is exclusive
        partition_ranges[row[col_name]] = (start, end - 1)
        start = end
    # As end is exclusive this is also the count of final partitions
    numPartitions = end

    # We could re-create a dataframe by passing this through
    # spark.createDataFrame(result, df.schema), but it's completely unnecessary
    # as the next step transforms into a string and pyspark doesn't have the
    # Dataset api yet so has to write strings via RDD.saveAsTextFile.
    return (
        df.rdd
        .map(lambda row: (row[col_name], row))
        .partitionBy(numPartitions, lambda value: random.randint(*partition_ranges[value]))
        .map(lambda pair: pair[1])
    )


def main(spark, source, field, output, limit_per_file):
    df_wikis = (
        spark.read.table('canonical_data.wikis')
        .select(F.col('database_code').alias('wikiid'), 'domain_name')
    )

    df = (
        spark.read.parquet(source)
        # Join df_wikis to transform project (en.wikibooks) into wikiid needed
        # to derive elasticsearch index name.
        .join(df_wikis.hint('broadcast'),
              # project is just domain name with '.org' stripped
              on=F.concat(F.col('project'), F.lit('.org')) == F.col('domain_name'))
        .select('wikiid', 'page_id', 'score')
    )

    (
        # Note that this returns an RDD, not a DataFrame
        unique_value_per_partition(df, limit_per_file, 'wikiid')
        .map(lambda row: documentData(row, field)) \
        .saveAsTextFile(output, compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')
    )


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-s', '--source', dest='source', metavar='SOURCE', help='source for the data')
    parser.add_argument('-f', '--field-name', dest='field', help='Name of elasticsearch field to populate')
    parser.add_argument('-o', '--output', dest='output', metavar='OUTPUT', help='Output to store results')
    parser.add_argument('-l', '--limit-per-file', dest='limit_per_file', metavar='N',
                        default=200000, help='Maximum number of records per output file')
    args = parser.parse_args()

    spark = SparkSession.builder.appName('Export {} to elasticsearch bulk format'.format(args.field)).getOrCreate()
    main(spark, **dict(vars(args)))
