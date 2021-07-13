"""Build bulk imports for elasticsearch with super_detect_noop

Configuration driven implementation of sourcing data from a
a variety of tables in hive and writing out scripted elasticsearch
updates.

Source partitions must have a single row per wiki page and include
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
This functionality is necessary as the full set of indices is spread across
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
from pyspark import RDD, SparkContext

from argparse import ArgumentParser
from collections import defaultdict
from dataclasses import dataclass, field as dataclass_field, InitVar
from datetime import datetime
import json
import logging
import math
import random
import sys
from typing import cast, Callable, Mapping, MutableSequence, Optional, Sequence, Set, Tuple, Union

from wmf_spark import HivePartition


def str_to_dt(val: str) -> datetime:
    # full iso-8601 matching airflow `ts` formatting for sanity purposes.
    return datetime.strptime(val, '%Y-%m-%dT%H:%M:%S+00:00')


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('-c', '--config', dest='config', metavar='CONFIG', choices=CONFIG.keys(),
                        help='Name of configuration to run')
    parser.add_argument('-o', '--output', dest='output', metavar='OUTPUT',
                        help='Output to store results')
    parser.add_argument('-l', '--limit-per-file', dest='limit_per_file', metavar='N', type=int,
                        default=200000, help='Maximum number of records per output file')
    parser.add_argument('-d', '--datetime', dest='dt', metavar='DATE', type=str_to_dt,
                        help='Date of input partitions in YYYY-MM-DDTHH:MM format. ex: 2001-01-15T19:00')
    parser.add_argument('-n', '--namespace-map-table', dest='namespace_map_table',
                        help='Table mapping wikiid + namespace_id to an elasticsearch index')
    return parser


def escape_hql_literal(value: str):
    # scala companion objects are funny to access from jvm, we need __getattr__
    # because the $ are not valid python.
    Literal = (
        SparkContext._jvm.org.apache.spark.sql.catalyst.expressions  # type: ignore
        .__getattr__('Literal$').__getattr__('MODULE$')
    )
    return Literal.apply(value).sql()


@dataclass
class Field:
    """Abstract class specifying a field to read

    Full implementation must also include a handler property.
    """
    # HQL Expression of the update content. Typically a field name.
    field: str
    # Name of field in elasticsearch
    alias: str

    @staticmethod
    def validate_equiv_fields(fields: Sequence[Field]):
        """Validate that fields defined in separate input sources are compatible

        The set of fields provided will have the same concrete type and alias,
        and be from separate data sources. This is primarily a pre-check that
        the `merge` method can be used. Implementations should log any issues
        with the configuration.
        """
        is_ok = True
        if len(fields) != 1:
            logging.critical(
                'Only one field with alias %s may be defined globally for type %s',
                fields[0].alias, type(fields[0]))
            is_ok = False
        return is_ok

    @property
    def column(self) -> Column:
        """Spark column applied to source table returning data to index"""
        return F.expr(self.field).alias(self.alias)

    @property
    def handler(self) -> str:
        raise NotImplementedError('Incomplete implementation, no handler defined')

    @staticmethod
    def merge(old_value: Column, new_value: Column) -> Column:
        """Merge fields provided by both sides of an input dataframe join

        Defined statically as the context of execution is not specific to
        a single instance.
        """
        raise NotImplementedError('No merge strategy implemented')


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


@dataclass
class MultiListField(Field):
    """Stores string inputs from multiple sources in a single list

    Implemented primarily to store predicted classes from multiple ML models in
    the same field. Data is stored and searchable in a single elasticsearch
    field while each prefix is updated independently.

    Specified field in hive table must be an array of strings.
    """
    # Prefix, typically the model name, to apply to each element of array
    # in source table. Prefixes must be unique per source. On update the
    # noop script removes all elements with matching prefix, and then adds
    # the new values.
    # Values accepted:
    # - None  - unprefixed, for legacy ores_articletopic data
    # - str   - constant prefix to apply to all rows in table
    # - tuple - An hql expression (often a field of the table) to source
    #           the prefix from, and the set of prefixes to expect.
    prefix: InitVar[Optional[Union[str, Tuple[str, Set[str]]]]] = None

    # Set of prefixes this field provides
    prefix_expectlist: Set[str] = dataclass_field(init=False)

    # hql expression of prefix and trailing /
    prefix_expression: str = dataclass_field(init=False)

    def __post_init__(self, prefix):
        if prefix is None:
            self.prefix_expectlist = set()
            self.prefix_expression = '""'
        elif isinstance(prefix, str):
            self.prefix_expectlist = {prefix}
            self.prefix_expression = escape_hql_literal(prefix + "/")
        else:
            field, self.prefix_expectlist = prefix
            self.prefix_expression = 'concat({}, "/")'.format(field)
            # Reject unexpected prefixes. Mostly this guarantees we only ship what is expected
            # When x is not in the expectlist NULL is returned.
            # TODO: This feels awkward...
            self.column_wrapper = lambda x: F.when(F.expr(field).isin(self.prefix_expectlist), x)

    def column_wrapper(self, value: Column) -> Column:
        """Wraps the final self.column value with additional logic

        Default implementation simply returns the input. Specific use cases
        replace this method on init if additional logic is necessary.
        """
        return value

    @staticmethod
    def validate_equiv_fields(fields: Sequence[Field]):
        # Not sure how to represent this in the type system, but
        # caller ensures all fields are of our concrete type.
        assert all(type(field) == MultiListField for field in fields)
        fields = cast(Sequence[MultiListField], fields)

        is_ok = True
        num_prefixes = sum(len(field.prefix_expectlist) for field in fields)
        num_unique = len(set.union(*(field.prefix_expectlist for field in fields)))
        if num_prefixes != num_unique:
            logging.critical(
                'All tables must have unique prefixes for multilist aliased to %s',
                fields[0].alias)
            is_ok = False
        return is_ok

    @property
    def column(self) -> Column:
        # F.expr is used as higher order functions aren't available through
        # the pyspark api directly.
        values = F.expr('transform({}, x -> concat({}, x))'.format(
            self.field, self.prefix_expression
        ))
        return self.column_wrapper(values).alias(self.alias)

    @staticmethod
    def merge(old_value: Column, new_value: Column) -> Column:
        """Merge fields provided by both sides of an input dataframe join"""
        # concat(null, array) and concat(array, null) both return null, so
        # we need a coalesce to find the non-null one.
        return F.coalesce(F.concat(old_value, new_value), old_value, new_value)

    @property
    def handler(self) -> str:
        return 'multilist'


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
    # Templated partition spec for loading data. Will receive named
    # parameters of table_name: str and dt: datetime.
    partition_spec_tmpl: str
    # Field in source table identifying the wiki, either `project` or `wikiid`
    join_on: str
    # String must be one of the keys to UPDATE_KINDS mapping
    update_kind: str
    # Definitions for the set of fields to source from this table
    fields: Sequence[Field]

    # registered partition spec templates. When key value is in
    # partition_spec_tmpl then this value will be used as the template.
    PARTITION_TMPL = {
        '@hourly': '{table_name}/year={dt.year}/month={dt.month}/day={dt.day}/hour={dt.hour}',
        '@daily': '{table_name}/year={dt.year}/month={dt.month}/day={dt.day}',
    }

    @property
    def handlers(self) -> Mapping[str, str]:
        """super_detect_noop handlers for fields defined in this table"""
        return {field.alias: field.handler for field in self.fields}

    @property
    def columns(self) -> Sequence[Column]:
        """Set of columns to source from this table"""
        return [field.column for field in self.fields]

    def partition_spec(self, dt: datetime) -> str:
        tmpl = self.partition_spec_tmpl
        if tmpl.startswith('@'):
            pieces = tmpl.split('/', 2)
            tmpl_prefix = self.PARTITION_TMPL[pieces[0]]
            tmpl = '/'.join([tmpl_prefix] + pieces[1:])
        return tmpl.format(table_name=self.table_name, dt=dt)

    def partition(self, dt: datetime) -> HivePartition:
        return HivePartition.from_spec(self.partition_spec(dt))

    def index_is_allowed(self, index_name: Column) -> Column:
        return UPDATE_KINDS[self.update_kind](index_name)

    def resolve_wikiid(self, df: DataFrame, df_wikis: DataFrame) -> DataFrame:
        return JOIN_ON[self.join_on](df, df_wikis)


# Constants for fields in elasticsearch
WEIGHTED_TAGS = 'weighted_tags'
POPULARITY_SCORE = 'popularity_score'


# Each map entry defines a named list of tables to load from hive and the
# fields to source from this table. This could load from an external yaml or
# maybe even a provided config.py, but for now this works and is minimally
# complicated.  The actual config is wrapped into a lambda as creating it may
# trigger hql escaping, which requires the spark jvm to have been initialized.
CONFIG: Mapping[str, Callable[[], Sequence[Table]]] = {
    'hourly': lambda: [
        Table(
            table_name='discovery.ores_articletopic',
            partition_spec_tmpl='@hourly/source=ores_predictions_hourly',
            join_on=JOIN_ON_WIKIID,
            update_kind=UPDATE_ALL,
            fields=[
                MultiListField(field='articletopic', alias=WEIGHTED_TAGS, prefix='classification.ores.articletopic'),
            ]
        ),
        Table(
            table_name='discovery.ores_drafttopic',
            partition_spec_tmpl='@hourly/source=ores_predictions_hourly',
            join_on=JOIN_ON_WIKIID,
            update_kind=UPDATE_ALL,
            fields=[
                MultiListField(field='drafttopic', alias=WEIGHTED_TAGS, prefix='classification.ores.drafttopic')
            ]
        ),
        Table(
            table_name='discovery.mediawiki_revision_recommendation_create',
            partition_spec_tmpl='@hourly',
            join_on=JOIN_ON_WIKIID,
            update_kind=UPDATE_CONTENT_ONLY,
            fields=[
                MultiListField(
                    # The only information to share about recommendations is if
                    # they exist, provide a constant expression as the field
                    # instead of awkwardly storing the repeated value in the table.
                    field='array("exists|1")',
                    alias=WEIGHTED_TAGS,
                    prefix=('concat("recommendation.", recommendation_type)', {'recommendation.link'})),
            ]
        ),
    ],
    'weekly': lambda: [
        Table(
            table_name='discovery.popularity_score',
            partition_spec_tmpl='@daily',
            join_on=JOIN_ON_PROJECT,
            update_kind=UPDATE_CONTENT_ONLY,
            fields=[
                WithinPercentageField(field='score', alias=POPULARITY_SCORE, percentage=20)
            ]
        ),
    ],
    'ores_bulk_ingest': lambda: [
        Table(
            table_name=t.table_name,
            partition_spec_tmpl='@hourly/source=ores_predictions_bulk_ingest',
            join_on=t.join_on,
            update_kind=t.update_kind,
            fields=t.fields)
        for t in CONFIG['hourly']()
        if t.table_name.startswith('discovery.ores_')
    ]
}


def validate_config(config: Sequence[Table]) -> bool:
    """Validate configuration sanity.

    Allows remaining parts to make strong assumptions about the configuration,
    particularly around fields with the same name from multiple sources having
    equivalent configuration.
    """
    if len(config) == 0:
        logging.critical('Empty configuration supplied!')
        return False

    fields: Mapping[str, MutableSequence[Field]] = defaultdict(list)
    is_ok = True
    for table in config:
        logging.info('Validating configuration for table %s', table.table_name)
        if table.join_on not in ('wikiid', 'project'):
            logging.critical('join_on condition must be either wikiid or project')
            is_ok = False
        if table.update_kind not in UPDATE_KINDS:
            logging.critical('Unknown update_kind of %s', table.update_kind)
            is_ok = False
        if len(table.fields) == 0:
            logging.critical('No fields defined for table')
            is_ok = False
        for field in table.fields:
            fields[field.alias].append(field)

    for shared_alias_fields in fields.values():
        head = shared_alias_fields[0]
        logging.info('Validating fields aliased to %s', head.alias)
        # When Field.merge is invoked we only have a Field implementation
        # representing the current data source. For this to work sanely we
        # must guarantee the values are produced in equivalent ways, making
        # them mergable.
        if all(type(field) == type(head) for field in shared_alias_fields):
            is_ok &= head.validate_equiv_fields(shared_alias_fields)
        else:
            logging.critical(
                'All fields with same alias must be of the same concrete type')
            is_ok = False

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


def prepare_merged_cols(
    # Set of column names in left side of the join that were provided by a Field impl.
    current_column_names: Set[str],
    # Set of Field impl defining right side of join.
    new_fields: Sequence[Field],
) -> Sequence[Column]:
    """Prepares a set of columns that will merge the fields after a join

    This is hard coded fairly specifically to the conditions necessary:

    Assumes that current_column_names are in a table aliased `left`, and
    new_fields are are a table aliased `right`.

    Assumes that validate_config has guaranteed if we see two columns with the
    same name (field alias) on either side of the join they must be equivalent,
    such that the Field.merge method for the new field can operate correctly
    with the old value.
    """
    right = set(field.alias for field in new_fields)
    # Should have been previously verified
    assert len(right) == len(new_fields)

    out = []
    for field in new_fields:
        if field.alias in current_column_names:
            # Field found on both sides of join
            field_column = field.merge(F.col('left.' + field.alias), F.col('right.' + field.alias))
        else:
            field_column = F.col('right.' + field.alias)
        out.append(field_column.alias(field.alias))
    # The set of columns in the current dataframe and not the joined dataframe.
    # current_column_names must only contain columns defined by Field impl, and
    # not fields like elastic_index
    for field_name in (current_column_names - right):
        out.append(F.col('left.' + field_name))

    return out


def prepare(
    spark: SparkSession, config: Sequence[Table],
    namespace_map_table: str, dt: datetime
) -> DataFrame:
    """Collect varied inputs into single dataframe"""
    df_wikis = (
        spark.read.table('canonical_data.wikis')
        .select('database_code', 'domain_name')
    )

    df_nsmap = spark.read.table(namespace_map_table)
    assert df_nsmap._jdf.isEmpty() is False  # type: ignore

    df: Optional[DataFrame] = None
    # Non-field columns that will exist on all dataframes
    shared_cols = {'wikiid', 'page_id', 'elastic_index'}
    for table in config:
        df_current = table.partition(dt).read(spark)
        df_current = table.resolve_wikiid(df_current, df_wikis)
        # We must resolve the cirrussearch index name now, rather than once
        # the tables have been combined, to allow each table to control the set
        # of indices it allow writes to.
        df_current = (
            resolve_cirrus_index(df_current, df_nsmap)
            .where(table.index_is_allowed(F.col('elastic_index')))
            # Each row must have at least one valid field for export
            .where(F.coalesce(*table.columns).isNotNull())
            .select(*shared_cols, *table.columns)
        )

        if df is None:
            df = df_current
        else:
            merged_cols = prepare_merged_cols(
                # Only pass columns defined by Field impl's
                set(df.columns) - shared_cols,
                table.fields)
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
    end = 0
    partition_ranges = {}
    for row in df.groupBy(df[col_name]).count().collect():
        end = int(start + math.ceil(row['count'] / limit_per_partition))
        # -1 is because randint is inclusive, and end is exclusive
        partition_ranges[row[col_name]] = (start, end - 1)
        start = end
    # As end is exclusive this is also the count of final partitions
    if end == 0:
        raise Exception('Empty dataframe provided')
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
    output: str,
    limit_per_file: int,
    dt: datetime,
    namespace_map_table: str,
    config: Union[str, Sequence[Table]],
) -> int:
    # Must be initialized before invoking CONFIG lambda
    # as hql escaping needs the jvm.
    spark = SparkSession.builder.getOrCreate()

    if isinstance(config, str):
        config = CONFIG[config]()
    if not validate_config(config):
        return 1

    df = prepare(
        spark=spark,
        config=config,
        namespace_map_table=namespace_map_table,
        dt=dt)

    # Assumes validate_config has ensured any overlaps in handler names
    # is reasonable and allowed to overwrite in arbitrary order.
    field_handlers = {k: v for table in config for k, v in table.handlers.items()}

    (
        # Note that this returns an RDD, not a DataFrame
        unique_value_per_partition(df, limit_per_file, 'wikiid')
        .map(lambda row: document_data(row, field_handlers)) \
        # Unfortunately saveAsTextFile doesn't have an overwrite option, so
        # this varies from other scripts in this repo.
        .saveAsTextFile(output, compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')
    )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
