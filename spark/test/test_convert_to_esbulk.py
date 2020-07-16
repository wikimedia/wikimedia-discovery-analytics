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
