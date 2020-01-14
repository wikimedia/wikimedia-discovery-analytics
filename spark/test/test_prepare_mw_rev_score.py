from pyspark.sql import functions as F, types as T

import prepare_mw_rev_score


def test_stringify_prediction_happy_path():
    results = prepare_mw_rev_score.stringify_prediction(
        {
            'good': 1.0,
            'middle': 0.54321,
            'bad': 0.2,
        },
        threshold=0.4)

    assert set(results) == {'good|1000', 'middle|543'}


def test_stringify_prediction_all_bad():
    results = prepare_mw_rev_score.stringify_prediction(
        {
            'bad': 0.2,
            'worse': 0.1,
        },
        threshold=0.4)

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
