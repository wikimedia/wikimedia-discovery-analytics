from pyspark.sql import functions as F, types as T
import pytest
import fulltext_head_queries


# Minimal schema for event.searchsatisfaction
SCHEMA = T.StructType([
    T.StructField('wiki', T.StringType()),
    T.StructField('event', T.StructType([
        T.StructField('source', T.StringType()),
        T.StructField('action', T.StringType()),
        T.StructField('query', T.StringType()),
        T.StructField('searchSessionId', T.StringType()),
    ])),
])


def serp(
    query, wiki='pytestwiki', source='fulltext',
    action='searchResultPage', sessionId='abcd'
):
    """Helper for constructing tuples matching SCHEMA"""
    return (wiki, (source, action, query, sessionId))


def test_does_not_count_autocomplete(spark):
    df = spark.createDataFrame([
        serp('pytest query', source='autocomplete')
    ], SCHEMA)

    rows = fulltext_head_queries.extract_head_queries(df, 10, 1).collect()
    assert len(rows) == 0


def test_filters_min_sessions(spark):
    df = spark.createDataFrame([
        serp('pytest query')
    ], SCHEMA)

    rows = fulltext_head_queries.extract_head_queries(df, 10, 2).collect()
    assert len(rows) == 0

    df = spark.createDataFrame([
        serp('pytest query', sessionId='a'),
        serp('pytest query', sessionId='b')
    ], SCHEMA)
    rows = fulltext_head_queries.extract_head_queries(df, 10, 2).collect()
    assert len(rows) == 1


def test_multiple_queries_from_same_session_counted_once(spark):
    df = spark.createDataFrame([
        serp('query a'), serp('query a')
    ], SCHEMA)

    rows = fulltext_head_queries.extract_head_queries(df, 10, 1).collect()
    assert len(rows) == 1
    assert rows[0].norm_query == 'query a'
    assert rows[0].num_sessions == 1


def test_similar_queries_as_same_query(spark):
    df = spark.createDataFrame([
        serp('query a'), serp('Query A?'),
    ], SCHEMA)

    rows = fulltext_head_queries.extract_head_queries(df, 10, 1).collect()
    assert len(rows) == 1
    assert rows[0].norm_query == 'query a'
    assert rows[0].num_sessions == 1


def test_counts_queries_from_multiple_sessions(spark):
    df = spark.createDataFrame([
        serp('query a', sessionId='abcd'),
        serp('query a', sessionId='zyxw'),
    ], SCHEMA)

    rows = fulltext_head_queries.extract_head_queries(df, 10, 1).collect()
    assert len(rows) == 1
    assert rows[0].num_sessions == 2


def test_sessions_count_each_query_separately(spark):
    df = spark.createDataFrame([
        serp('query a'), serp('query b')
    ], SCHEMA)

    rows = fulltext_head_queries.extract_head_queries(df, 10, 1).collect()
    assert len(rows) == 2
    assert all(row.num_sessions == 1 for row in rows)


@pytest.mark.parametrize('source,expected', [
    ('plain text is unchanged', 'plain text is unchanged'),
    ('Queries will be lowercased', 'queries will be lowercased'),
    ('flattens    multiple  spaces', 'flattens multiple spaces'),
    ('  leading and trailing spaces removed  ', 'leading and trailing spaces removed'),
    ('strips !! punctuation?', 'strips punctuation'),
    ('__maintains_spacing_when_cleaning', 'maintains spacing when cleaning'),
    # implicitly the next two assert diacritics are maintained
    ('lowercased utf8 unchanged διακριτικός', 'lowercased utf8 unchanged διακριτικός'),
    ('lowercases utf8 ΔΙΑΚΡΙΤΙΚΌΣ', 'lowercases utf8 διακριτικός'),
])
def test_norm_query(spark, source, expected):
    norm_query = (
        spark.range(1)
        .select(fulltext_head_queries.norm_query(F.lit(source)).alias('norm_query'))
        .head()
        .norm_query
    )

    assert norm_query == expected
