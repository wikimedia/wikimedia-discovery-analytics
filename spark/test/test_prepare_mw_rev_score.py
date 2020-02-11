from pyspark.sql import functions as F, types as T
import pytest

import prepare_mw_rev_score


@pytest.fixture
def stringify_prediction():
    return prepare_mw_rev_score.make_stringify_prediction({
        # wiki -> label -> minimum acceptable threshold
        'pytestwiki': {
            'good': 0.5,
            'middle': 0.5,
            'bad': 0.5,
            'worse': 0.5
        }
    })


def test_stringify_prediction_happy_path(stringify_prediction):
    results = stringify_prediction('pytestwiki', {
        'good': 1.0,
        'middle': 0.54321,
        'bad': 0.2,
    })

    assert set(results) == {'good|1000', 'middle|543'}
    assert len(set(results)) == len(results)


def test_stringify_prediction_all_bad(stringify_prediction):
    results = stringify_prediction('otherwiki', {
        'bad': 0.2,
        'worse': 0.1,
    })

    assert len(results) == 0


def test_top_row_per_group(spark):
    df = spark.createDataFrame([
        ('a', 1),
        ('a', 10),
        ('b', 1),
        ('c', 4),
    ], T.StructType([
        T.StructField('group', T.StringType()),
        T.StructField('order', T.IntegerType()),
    ]))

    results = prepare_mw_rev_score.top_row_per_group(
        df, ['group'], [F.col('order').desc()]
    ).collect()

    results = [(r.group, r.order) for r in results]
    assert set(results) == {('a', 10), ('b', 1), ('c', 4)}
    assert len(results) == len(set(results))
