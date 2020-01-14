# A very primitive script to transfer from hive data files
# to elasticsearch bulk import formatted files on swift.
from __future__ import print_function
try:
    from pyspark.sql import SparkSession
except ImportError:
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, Row, functions as F
from pyspark import RDD
from argparse import ArgumentParser
from datetime import datetime
import json
import math
import random
import sys
from typing import cast, Dict, Mapping, NamedTuple, Sequence, Tuple


Field = NamedTuple('Field', [('field', str), ('alias', str), ('handler', str)])
Table = NamedTuple('Table', [
    ('table', str), ('join_on', str), ('fields', Sequence[Field])])

# Each entry defines a table to load from hive and the fields to source
# from this table.
CONFIG = [
    Table(
        table='discovery.popularity_score',
        join_on='project',
        fields=[
            Field(field='score', alias='popularity_score', handler='within 20%')
        ]
    ),
    Table(
        table='discovery.ores_drafttopic',
        join_on='wikiid',
        fields=[
            Field(field='drafttopic', alias='ores_drafttopics', handler='equals')
        ]
    ),
]


def prepare_table(
    df: DataFrame, df_wikis: DataFrame, table: Table
) -> DataFrame:
    """Convert input dataframe into expected shape

    Adjusts the table, per configuration, to contain the wikiid, page_id, and
    fields selected for upload. Input data must be a row-per-page and can identify
    the wiki either by db name (ex: eswiki) or project (ex: es.wikipedia).
    """
    if table.join_on == 'project':
        df = df.join(
            df_wikis.hint('broadcast'),
            on=F.concat(F.col('project'), F.lit('.org')) == F.col('domain_name'))
    elif table.join_on != 'wikiid':
        raise ValueError('join_on must be in (project, wikiid): {}'.format(table.join_on))

    cols = [F.col(field.field).alias(field.alias) for field in table.fields]

    return df.select('wikiid', 'page_id', *cols)


def prepare(
    spark: SparkSession, date: datetime, config: Sequence[Table]
) -> Tuple[DataFrame, Mapping[str, str]]:
    """Collect varied inputs into single dataframe"""
    df_wikis = (
        spark.read.table('canonical_data.wikis')
        .select(F.col('database_code').alias('wikiid'), 'domain_name')
    )

    current_partition = (F.col('year') == date.year) \
        & (F.col('month') == date.month) & (F.col('day') == date.day)

    df = None
    field_handlers = cast(Dict[str, str], {})

    for table in config:
        new_handlers = {field.alias: field.handler for field in table.fields}
        if any(name in field_handlers for name in new_handlers.keys()):
            raise ValueError('Duplicate field handlers configured')
        field_handlers.update(new_handlers)

        df_current = prepare_table(
            spark.read.table(table.table).where(current_partition),
            df_wikis, table)

        if df is None:
            df = df_current
        else:
            df = df.join(df_current, how='outer', on=['wikiid', 'page_id'])
    assert df is not None
    return df, field_handlers


def unique_value_per_partition(
        df: DataFrame, limit_per_partition: int, col_name: str
) -> RDD:
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

    def partitioner(value: str) -> int:
        return random.randint(*partition_ranges[value])

    # We could re-create a dataframe by passing this through
    # spark.createDataFrame(result, df.schema), but it's completely unnecessary
    # as the next step transforms into a string and pyspark doesn't have the
    # Dataset api yet so has to write strings via RDD.saveAsTextFile.
    return (
        df.rdd
        .map(lambda row: (row[col_name], row))
        .partitionBy(numPartitions, partitioner)
        .map(lambda pair: pair[1])
    )


def _document_data(
    document: Row, handlers: Mapping[str, str]
) -> Tuple[Mapping, Mapping]:
    """Transform document into elasticsearch bulk import request structures"""
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
                    field: handler
                    for field, handler in handlers.items()
                    if document[field] is not None
                },
                'source': {
                    field: document[field]
                    for field in handlers.keys()
                    if document[field] is not None
                }
            }
        }
    }

    return header, update


def document_data(document: Row, handlers: Mapping[str, str]) -> str:
    """Transform document into elasticsearch bulk import lines"""
    header, update = _document_data(document, handlers)
    return '{}\n{}'.format(json.dumps(header), json.dumps(update))


def main(
    spark: SparkSession,
    output: str,
    limit_per_file: int,
    date: datetime,
    config: Sequence[Table] = CONFIG
) -> int:
    df, field_handlers = prepare(spark, date, config)
    (
        # Note that this returns an RDD, not a DataFrame
        unique_value_per_partition(df, limit_per_file, 'wikiid')
        .map(lambda row: document_data(row, field_handlers)) \
        .saveAsTextFile(output, compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')
    )
    return 0


if __name__ == "__main__":
    def date(val):
        return datetime.strptime(val, '%Y-%m-%d')

    parser = ArgumentParser()
    parser.add_argument('-o', '--output', dest='output', metavar='OUTPUT', help='Output to store results')
    parser.add_argument('-l', '--limit-per-file', dest='limit_per_file', metavar='N', type=int,
                        default=200000, help='Maximum number of records per output file')
    parser.add_argument('-d', '--date', dest='date', metavar='DATE', type=date,
                        help='Date of input partitions in YYYY-MM-DD format')
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    sys.exit(main(spark, **dict(vars(args))))
