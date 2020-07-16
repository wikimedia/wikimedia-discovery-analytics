"""Build bulk imports for elasticsearch with super_detect_noop

Configuration driven implementation of sourcing data from a
a variety of tables in hive and writing out scripted elasticsearch
updates.

Source partitions must have a single column per wiki page, and include
the columns `page_id` and `page_namespace`. To identify the wiki they
must include either the wiki database name as `wikiid` or the leading
portion of the domain name (ex: `ar.wikipedia`) as `project`.

Individually configured fields are read from each source partition and
merged together into a single update per page. All updates are wrapped
with the super_detect_noop script from wmf search-extra plugin. This
plugin provides context sensitive updates that look at the data already
stored in elastic when deciding how/if the update should be applied.

Final output is a series of gzip compressed text files formatted as bulk import
lines (application/x-ndjson). Each file is guaranteed to contain updates for a
single wiki. It may have updates for multiple indices assigned to that wiki.
This funcionality is necessary as the full set of indices is spread across
multiple clusters, but each wiki in its entirety is found within a single
cluster.
"""
from __future__ import annotations
try:
    from pyspark.sql import SparkSession
except ImportError:
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
from pyspark.sql import Column, DataFrame, Row, functions as F
from pyspark import RDD

from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime
import json
import logging
import math
import random
import sys
from typing import Callable, Mapping, Optional, Sequence, Set, Tuple


@dataclass
class Field:
    """Abstract class specifying a field to read

    Full implementation must also include a handler property.
    """
    # Name of field in source table
    field: str
    # Name of field in elasticsearch
    alias: str

    @property
    def column(self) -> Column:
        """Spark column applied to source table returning data to index"""
        return F.col(self.field).alias(self.alias)

    @property
    def handler(self) -> str:
        raise NotImplementedError('Incomplete implementation, no handler defined')


@dataclass
class EqualsField(Field):
    """Updates data when stored value is not exactly equal"""

    @property
    def handler(self) -> str:
        return 'equals'


@dataclass
class WithinPercentageField(Field):
    """Noops update to numeric data when stored value is within the percentage"""

    # Percentage, from 0-100 exclusive, numeric update must differ from the
    # value already stored in elasticsearch to trigger an update.
    percentage: int

    @property
    def handler(self) -> str:
        assert 0 < self.percentage < 100
        return 'within {}%'.format(self.percentage)


UPDATE_CONTENT_ONLY = 'content_only'
UPDATE_ALL = 'all'

# Each value is a function receiving Column representing the CirrusSearch
# index to write to. Returns a Column representing a boolean indicating if
# the write will be allowed.
UPDATE_KINDS: Mapping[str, Callable[[Column], Column]] = {
    UPDATE_CONTENT_ONLY: lambda col: col.endswith('_content'),
    UPDATE_ALL: lambda col: F.lit(True)
}

# ex: es.wiktionary
JOIN_ON_PROJECT = 'project'
# ex: eswiki
JOIN_ON_WIKIID = 'wikiid'

# Defines how the data should be joined to the full dataset of updates which
# are keyed by wikiid. All inputs must already have page_id and page_namespace
# fields to be merged.
# Each value is a function receiving a DataFrame of per-page rows, and a
# dataframe containing the `canonical_data.wikis` table. The result rows must
# have a 'wikiid' field containing the database_name of the wiki the page
# belongs to.
JOIN_ON: Mapping[str, Callable[[DataFrame, DataFrame], DataFrame]] = {
    JOIN_ON_WIKIID: lambda df, df_wikis: df,
    JOIN_ON_PROJECT: lambda df, df_wikis: df.join(
        F.broadcast(df_wikis).withColumnRenamed('database_code', 'wikiid'),
        on=F.concat(F.col('project'), F.lit('.org')) == F.col('domain_name')),
}


@dataclass
class Table:
    # Fully qualified hive table to read from
    table_name: str
    # Field in source table identifying the wiki, either `project` or `wikiid`
    join_on: str
    # String must be one of the keys to UPDATE_KINDS mapping
    update_kind: str
    # Definitions for the set of fields to source from this table
    fields: Sequence[Field]

    @property
    def handlers(self) -> Mapping[str, str]:
        """super_detect_noop handlers for fields defined in this table"""
        return {field.alias: field.handler for field in self.fields}

    @property
    def columns(self) -> Sequence[Column]:
        """Set of columns to source from this table"""
        return [field.column for field in self.fields]

    def index_is_allowed(self, index_name: Column) -> Column:
        return UPDATE_KINDS[self.update_kind](index_name)

    def resolve_wikiid(self, df: DataFrame, df_wikis: DataFrame) -> DataFrame:
        return JOIN_ON[self.join_on](df, df_wikis)


# Each entry defines a table to load from hive and the fields to source from
# this table. This could load from an external yaml or similar, but for now
# this works and is less complicated.
CONFIG = [
    Table(
        table_name='discovery.popularity_score',
        join_on=JOIN_ON_PROJECT,
        update_kind=UPDATE_CONTENT_ONLY,
        fields=[
            WithinPercentageField(field='score', alias='popularity_score', percentage=20)
        ]
    ),
    Table(
        table_name='discovery.ores_articletopic',
        join_on=JOIN_ON_WIKIID,
        update_kind=UPDATE_ALL,
        fields=[
            EqualsField(field='articletopic', alias='ores_articletopic')
        ]
    )
]


def validate_config(config: Sequence[Table]) -> bool:
    """Validate configuration sanity.

    Allows remaining parts to make strong assumptions about the configuration,
    particularly around fields with the same name from multiple sources having
    equivalent configuration.
    """
    seen: Set[str] = set()
    for table in config:
        logging.info('Validating configuration for table %s', table.table_name)
        if table.join_on not in ('wikiid', 'project'):
            logging.critical('join_on condition must be either wikiid or project')
            is_ok = False
        if table.update_kind not in UPDATE_KINDS:
            logging.critical('Unknown update_kind of %s', table.update_kind)
            is_ok = False
        for field in table.fields:
            if field.alias in seen:
                logging.critical('Duplicate field alias %s', field.alias)
                is_ok = False
            seen.add(field.alias)

    return is_ok


def daily_partition(dt: datetime) -> Column:
    """Boolean condition for single daily partition"""
    return (F.col('year') == dt.year) \
        & (F.col('month') == dt.month) \
        & (F.col('day') == dt.day)


def resolve_cirrus_index(df: DataFrame, df_nsmap: DataFrame) -> DataFrame:
    """Determine cirrussearch index to write to."""
    return (
        df
        .withColumnRenamed('page_namespace', 'namespace_id')
        .join(
            F.broadcast(df_nsmap),
            how='inner', on=['wikiid', 'namespace_id'])
        .drop('namespace_id')
    )


def prepare(
    spark: SparkSession, config: Sequence[Table],
    namespace_map_table: str, partition_cond: Column
) -> DataFrame:
    """Collect varied inputs into single dataframe"""
    df_wikis = (
        spark.read.table('canonical_data.wikis')
        .select('database_code', 'domain_name')
    )

    df_nsmap = spark.read.table(namespace_map_table)
    df: Optional[DataFrame] = None
    # Non-field columns that will exist on all dataframes
    shared_cols = {'wikiid', 'page_id', 'elastic_index'}
    for table in config:
        df_current = spark.read.table(table.table_name).where(partition_cond)
        df_current = table.resolve_wikiid(df_current, df_wikis)
        # We must resolve the cirrussearch index name now, rather than once
        # the tables have been combined, to allow each table to control the set
        # of indices it allow writes to.
        df_current = (
            resolve_cirrus_index(df_current, df_nsmap)
            .where(table.index_is_allowed(F.col('elastic_index')))
            .select(*shared_cols, *table.columns))

        if df is None:
            df = df_current
        else:
            # validate_configuration previously verified this is all unique
            merged_cols = set(df.columns + df_current.columns) - shared_cols
            df = (
                df.alias('left')
                .join(df_current.alias('right'),
                      how='outer', on=['wikiid', 'page_id'])
                .select('wikiid', 'page_id',
                        # elastic_index is on both sides, but not part of the
                        # unique identifier. When both sides are available they
                        # should typically be the same. Take whichever one is
                        # available.
                        F.coalesce(
                            F.col('left.elastic_index'),
                            F.col('right.elastic_index')
                        ).alias('elastic_index'),
                        *merged_cols)
            )

    assert df is not None
    return df


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
    document: Row, handlers: Mapping[str, str],
) -> Tuple[Mapping, Mapping]:
    """Transform document into elasticsearch bulk import request structures"""
    header = {
        'update': {
            '_index': document.elastic_index,
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
    namespace_map_table: str,
    config: Sequence[Table] = CONFIG
) -> int:
    if not validate_config(config):
        return 1

    df = prepare(
        spark=spark,
        config=config,
        namespace_map_table=namespace_map_table,
        partition_cond=daily_partition(date))

    # Assumes validate_config has ensured any overlaps in handler names
    # is reasonable and allowed to overwrite in arbitrary order.
    field_handlers = {k: v for table in config for k, v in table.handlers.items()}

    (
        # Note that this returns an RDD, not a DataFrame
        unique_value_per_partition(df, limit_per_file, 'wikiid')
        .map(lambda row: document_data(row, field_handlers)) \
        # Unfortunately saveAsTextFile doesn't have an overwrite option, so this varies
        # other scripts in this repo.
        .saveAsTextFile(output, compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')
    )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    def date(val: str) -> datetime:
        return datetime.strptime(val, '%Y-%m-%d')

    parser = ArgumentParser()
    parser.add_argument('-o', '--output', dest='output', metavar='OUTPUT', help='Output to store results')
    parser.add_argument('-l', '--limit-per-file', dest='limit_per_file', metavar='N', type=int,
                        default=200000, help='Maximum number of records per output file')
    parser.add_argument('-d', '--date', dest='date', metavar='DATE', type=date,
                        help='Date of input partitions in YYYY-MM-DD format')
    parser.add_argument('-n', '--namespace-map-table', dest='namespace_map_table',
                        help='Table mapping wikiid + namespace_id to an elasticsearch index')
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    sys.exit(main(spark, **dict(vars(args))))
