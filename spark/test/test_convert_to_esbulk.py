import convert_to_esbulk
from pyspark.sql import Row, functions as F, types as T
import pytest


@pytest.fixture
def df_wikis(spark):
    rdd = spark.sparkContext.parallelize([
        ['testwiki', 'test.wikipedia.org'],
        ['zhwiki', 'zh.wikipedia.org'],
    ])
    return spark.createDataFrame(rdd, T.StructType([
        T.StructField('wikiid', T.StringType()),
        T.StructField('domain_name', T.StringType()),
    ]))


def test_document_data_happy_path():
    row = Row(wikiid='pytestwiki', page_id=5, custom="example")
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
    row = Row(wikiid='pytestwiki', page_id=6, a=None, b=20)
    handlers = {
        'a': 'equals',
        'b': 'equals'
    }
    header, update = convert_to_esbulk._document_data(row, handlers)
    for key in ('handlers', 'source'):
        assert 'a' not in update['script']['params'][key]
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


def test_prepare_table_join_on_project(spark, df_wikis):
    rdd = spark.sparkContext.parallelize([
        ['test.wikipedia', 42, 'indexed content']
    ])
    df = spark.createDataFrame(rdd, T.StructType([
        T.StructField('project', T.StringType()),
        T.StructField('page_id', T.IntegerType()),
        T.StructField('foo', T.StringType())
    ]))

    table = convert_to_esbulk.Table(
        table='pytest_example_table',
        join_on='project',
        fields=[
            convert_to_esbulk.Field(field='foo', alias='bar', handler='equals')
        ])

    df_result = convert_to_esbulk.prepare_table(
        df, df_wikis, table)
    # Project converted to wikiid
    # foo aliased to bar
    assert {'wikiid', 'page_id', 'bar'} == set(df_result.columns)

    results = df_result.collect()
    # The join must have kept our row
    assert len(results) == 1
    # Verify join resolved correct wikiid
    assert results[0].wikiid == 'testwiki'
    assert results[0].page_id == 42
    # Verify alias contains same data
    assert results[0].bar == 'indexed content'
