import convert_to_esbulk
from pyspark.sql import Row, functions as F, types as T
import pytest


# Used where namespace is expected for clarity
NS_MAIN = 0
NS_TALK = 1


@pytest.fixture
def df_wikis(spark):
    """Minimal match of canonical_data.wikis table"""
    rdd = spark.sparkContext.parallelize([
        ['testwiki', 'test.wikipedia.org'],
        ['zhwiki', 'zh.wikipedia.org'],
    ])
    return spark.createDataFrame(rdd, T.StructType([
        T.StructField('database_code', T.StringType()),
        T.StructField('domain_name', T.StringType()),
    ]))


@pytest.fixture
def df_namespace_map(spark):
    rdd = spark.sparkContext.parallelize([
        ('testwiki', NS_MAIN, 'testwiki_content'),
        ('testwiki', NS_TALK, 'testwiki_general'),
        ('zhwiki', NS_MAIN, 'zhwiki_content'),
        ('zhwiki', NS_TALK, 'zhwiki_general'),
    ])
    return spark.createDataFrame(rdd, T.StructType([
        T.StructField('wikiid', T.StringType()),
        T.StructField('namespace_id', T.StringType()),
        T.StructField('elastic_index', T.StringType())
    ]))


@pytest.fixture
def table_to_convert(spark):
    rdd = spark.sparkContext.parallelize([
        ['test.wikipedia', 42, NS_MAIN, 'indexed content'],
        ['test.wikipedia', 43, NS_TALK, 'indexed talkpage']
    ])
    df = spark.createDataFrame(rdd, T.StructType([
        T.StructField('project', T.StringType()),
        T.StructField('page_id', T.IntegerType()),
        T.StructField('page_namespace', T.IntegerType()),
        T.StructField('foo', T.StringType()),
    ]))

    table_def = convert_to_esbulk.Table(
        table_name='pytest_example_table',
        join_on=convert_to_esbulk.JOIN_ON_PROJECT,
        update_kind=convert_to_esbulk.UPDATE_ALL,
        fields=[
            convert_to_esbulk.EqualsField(field='foo', alias='bar')
        ])

    return table_def, df


def test_document_data_happy_path():
    row = Row(wikiid='pytestwiki', page_id=5,
              elastic_index='pytestwiki_content', custom="example")
    handlers = {
        'custom': 'within 20%',
    }
    header, update = convert_to_esbulk._document_data(row, handlers)
    assert header['update']['_index'] == 'pytestwiki_content'
    assert header['update']['_id'] == 5

    for key in ('handlers', 'source'):
        assert 'custom' in update['script']['params'][key]
        assert len(update['script']['params'][key]) == 1


def test_document_data_null_fields():
    row = Row(wikiid='pytestwiki', elastic_index='pytestwiki_content',
              page_id=6, a=None, b=20)
    handlers = {
        'a': 'equals',
        'b': 'equals'
    }
    header, update = convert_to_esbulk._document_data(row, handlers)
    for key in ('handlers', 'source'):
        # These properties should not have snuck into the update, they
        # are only for decision making
        for k in ('wikiid', 'elastic_index', 'page_id'):
            assert k not in update['script']['params'][key]
        # a is None, should not be shipped as an update
        assert 'a' not in update['script']['params'][key]
        # b must have values assigned
        assert 'b' in update['script']['params'][key]
        assert len(update['script']['params'][key]) == 1


def test_unique_value_per_partition(spark):
    num_rows = 100
    num_partition_keys = 5
    per_partition = 5

    df = (
        spark.range(100)
        .withColumn('partition_key', F.col('id') % num_partition_keys)
    )

    rdd = convert_to_esbulk.unique_value_per_partition(
        df, limit_per_partition=per_partition,
        col_name='partition_key')

    def collector(partition_index, iterator):
        # Convert each partition into a single row
        return [(partition_index, list(iterator))]

    partitions = rdd.mapPartitionsWithIndex(collector).collect()
    for idx, rows in partitions:
        partition_keys = {row.partition_key for row in rows}
        # Each partition must one or zero partition keys. We allow
        # zero items since we have a tiny dataset and the split
        # is fed by randomness
        assert len(partition_keys) <= 1
        # While we asked for 5 items per partition, it's based on
        # randomness and 5 items isn't enough for randomness to
        # give us very close numbers. For now assert that we didn't
        # end up with all values in the same partition at least.
        assert len(rows) < (num_rows / num_partition_keys)


def test_join_on_project(df_wikis, table_to_convert):
    table_def, df = table_to_convert

    df_result = table_def.resolve_wikiid(df, df_wikis)
    assert 'wikiid' in df_result.columns

    rows = df_result.collect()
    wikis = {row.wikiid for row in rows}
    assert wikis == {'testwiki'}


def test_prepare_happy_path(
    mocker, spark, df_wikis, df_namespace_map, table_to_convert
):
    table_def, df = table_to_convert
    namespace_map_table = 'pytest_cirrus_nsmap'

    spark = mocker.MagicMock()
    spark.read.table.side_effect = lambda table_name: {
        namespace_map_table: df_namespace_map,
        'canonical_data.wikis': df_wikis,
        table_def.table_name: df
    }[table_name]

    df_result = convert_to_esbulk.prepare(
        spark=spark, config=[table_def],
        namespace_map_table=namespace_map_table,
        partition_cond=F.lit(True))

    # project resolved to wikiid and dropped. foo aliased to bar.
    # page_namespace transformed into elastic_index and dropped.
    # page_id passed through.
    assert {'elastic_index', 'wikiid', 'page_id', 'bar'} == set(df_result.columns)

    results = df_result.collect()
    assert len(results) == 2, "The join must have kept our rows"
    results = list(sorted(results, key=lambda x: x.page_id))
    assert results[0].wikiid == 'testwiki', 'Resolved correct wikiid'
    assert results[0].elastic_index == 'testwiki_content', \
        'Resolved correct elastic index'
    assert results[0].page_id == 42, "Page id unchanged"
    assert results[0].bar == 'indexed content', "content appropriately aliased"

    assert results[1].wikiid == 'testwiki'
    assert results[1].page_id == 43
    assert results[1].elastic_index == 'testwiki_general'


@pytest.mark.parametrize('is_valid,msg,config', [
    (False, "must define at least one table", []),
    (True, "happy path", [convert_to_esbulk.Table(
        table_name='pytest_table',
        join_on=convert_to_esbulk.JOIN_ON_WIKIID,
        update_kind=convert_to_esbulk.UPDATE_ALL,
        fields=[
            convert_to_esbulk.EqualsField(field='field', alias='alias')
        ]
    )]),
    (False, "table must have fields", [convert_to_esbulk.Table(
        table_name='pytest_table',
        join_on=convert_to_esbulk.JOIN_ON_WIKIID,
        update_kind=convert_to_esbulk.UPDATE_ALL,
        fields=[]
    )]),
    (False, "update_kind must be valid", [convert_to_esbulk.Table(
        table_name='pytest_table',
        join_on=convert_to_esbulk.JOIN_ON_WIKIID,
        update_kind='does-not-exist',
        fields=[
            convert_to_esbulk.EqualsField(field='field', alias='alias'),
        ]
    )]),
    (False, "join_on must be valid", [convert_to_esbulk.Table(
        table_name='pytest_table',
        join_on='does-not-exist',
        update_kind=convert_to_esbulk.UPDATE_ALL,
        fields=[
            convert_to_esbulk.EqualsField(field='field', alias='alias'),
        ]
    )]),
    (True, "multilist allows unique prefixes", [convert_to_esbulk.Table(
        table_name='pytest_table',
        join_on=convert_to_esbulk.JOIN_ON_WIKIID,
        update_kind=convert_to_esbulk.UPDATE_ALL,
        fields=[
            convert_to_esbulk.MultiListField(field='field', alias='alias', prefix='a'),
            convert_to_esbulk.MultiListField(field='field', alias='alias', prefix='b'),
        ]
    )]),
    (False, "multilist rejects duplicate prefixes", [convert_to_esbulk.Table(
        table_name='pytest_table',
        join_on=convert_to_esbulk.JOIN_ON_WIKIID,
        update_kind=convert_to_esbulk.UPDATE_ALL,
        fields=[
            convert_to_esbulk.MultiListField(field='field', alias='alias', prefix='dupe'),
            convert_to_esbulk.MultiListField(field='field', alias='alias', prefix='dupe'),
        ]
    )]),
    (True, "alias duplication limits are per-alias", [convert_to_esbulk.Table(
        table_name='pytest_table',
        join_on=convert_to_esbulk.JOIN_ON_WIKIID,
        update_kind=convert_to_esbulk.UPDATE_ALL,
        fields=[
            convert_to_esbulk.MultiListField(field='field', alias='one', prefix='dupe'),
            convert_to_esbulk.MultiListField(field='field', alias='two', prefix='dupe'),
        ]
    )]),
])
def test_validate_config(is_valid, msg, config):
    assert is_valid == convert_to_esbulk.validate_config(config), msg


def test_multilist_unprefixed(mocker, df_wikis, df_namespace_map, table_to_convert):
    source_table_def, source_df = table_to_convert
    mocked_tables = {
        'canonical_data.wikis': df_wikis,
        'mock_namespace_map_table': df_namespace_map,
        'source_table': source_df.withColumn('foo', F.lit('a')),
    }

    spark = mocker.MagicMock()
    spark.read.table.side_effect = lambda table: mocked_tables[table]

    table_defs = [
        convert_to_esbulk.Table(
            table_name='source_table',
            join_on=convert_to_esbulk.JOIN_ON_PROJECT,
            # content_only limits test to single row
            update_kind=convert_to_esbulk.UPDATE_CONTENT_ONLY,
            fields=[
                convert_to_esbulk.MultiListField(field='foo', alias='bar', prefix=None)
            ]),
    ]
    df_result = convert_to_esbulk.prepare(
        spark=spark, config=table_defs,
        namespace_map_table='mock_namespace_map_table',
        partition_cond=F.lit(True))

    rows = df_result.collect()
    assert len(rows) == 1, "one row in, one row out"
    assert rows[0].bar == 'a', "Output should be unprefixed"


def test_multiple_multilist(mocker, df_wikis, df_namespace_map, table_to_convert):
    source_table_def, source_df = table_to_convert

    # Transform the source_df into two dataframes that should have multilist
    # values merged.
    df_a = source_df.withColumn('foo', F.array(F.lit('z'), F.lit('y')))
    df_b = source_df.withColumn('foo', F.array(F.lit('y'), F.lit('x')))

    mocked_tables = {
        'canonical_data.wikis': df_wikis,
        'mock_namespace_map_table': df_namespace_map,
        'pytest_example_table_a': df_a,
        'pytest_example_table_b': df_b,
    }

    spark = mocker.MagicMock()
    spark.read.table.side_effect = lambda table: mocked_tables[table]

    table_defs = [
        convert_to_esbulk.Table(
            table_name='pytest_example_table_a',
            join_on=convert_to_esbulk.JOIN_ON_PROJECT,
            # content_only limits test to single row
            update_kind=convert_to_esbulk.UPDATE_CONTENT_ONLY,
            fields=[
                convert_to_esbulk.MultiListField(field='foo', alias='bar', prefix='a')
            ]),
        convert_to_esbulk.Table(
            table_name='pytest_example_table_b',
            join_on=convert_to_esbulk.JOIN_ON_PROJECT,
            update_kind=convert_to_esbulk.UPDATE_CONTENT_ONLY,
            fields=[
                convert_to_esbulk.MultiListField(field='foo', alias='bar', prefix='b')
            ]),
    ]

    df_result = convert_to_esbulk.prepare(
        spark=spark, config=table_defs,
        namespace_map_table='mock_namespace_map_table',
        partition_cond=F.lit(True))

    rows = df_result.collect()
    assert len(rows) == 1, "Both tables should have been merged into single row"
    assert len(rows[0].bar) == len(set(rows[0].bar)), \
        "Unique inputs were provided, all outputs should be unique as well"
    assert set(rows[0].bar) == {'a/z', 'a/y', 'b/y', 'b/x'}, \
        "Both sources should be represented in the result"
