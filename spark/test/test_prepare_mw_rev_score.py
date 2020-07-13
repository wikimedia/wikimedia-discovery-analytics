from pyspark.sql import functions as F, types as T
import pytest

import prepare_mw_rev_score


# Used to clarify where namespaces are used
NS_MAIN = 0
NS_TALK = 1


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


@pytest.mark.parametrize('predictions,wbitems,expected', [
    [
        # predictions
        [
            # prediction to propagate
            ('pytestwiki', 42, NS_MAIN, ['pred|123']),
            # prediction not linked, will not be propagated
            ('pytestwiki', 1, NS_MAIN, ['pred|654']),
            # prediction from non-preferred wiki, will not be propagated
            ('secondwiki', 22, NS_MAIN, ['pred|555']),
        ],
        # wikibase_item properties
        [
            # Linked rows to propagate over
            ('pytestwiki', 42, NS_MAIN, 'Q987'),
            ('otherwiki', 11, NS_TALK, 'Q987'),
            # random extra data that gets ignored
            ('otherwiki', 15, NS_TALK, 'Q2'),
            # link between secondwiki and otherwiki, will not be followed
            # as secondwiki is not preferred.
            ('secondwiki', 22, NS_MAIN, 'Q9'),
            ('otherwiki', 99, NS_TALK, 'Q9'),
        ],
        # Expected set of propagated predictions
        {
            # original predictions retained
            ('pytestwiki', 42, NS_MAIN, ('pred|123',)),
            ('pytestwiki', 1, NS_MAIN, ('pred|654',)),
            ('secondwiki', 22, NS_MAIN, ('pred|555',)),
            # Additional propagated predictions
            ('otherwiki', 11, NS_TALK, ('pred|123',)),
        }
    ]
])
def test_propagate_by_wbitem(spark, predictions, wbitems, expected):
    df_predictions = spark.createDataFrame(predictions, T.StructType([
        T.StructField('wikiid', T.StringType()),
        T.StructField('page_id', T.IntegerType()),
        T.StructField('page_namespace', T.IntegerType()),
        T.StructField('prediction', T.ArrayType(T.StringType())),
    ]))

    df_wbitem = spark.createDataFrame(wbitems, T.StructType([
        T.StructField('wikiid', T.StringType()),
        T.StructField('page_id', T.IntegerType()),
        T.StructField('page_namespace', T.IntegerType()),
        T.StructField('wikibase_item', T.StringType()),
    ]))

    source_wikis = {'pytestwiki', 'secondwiki'}
    preferred_wiki = 'pytestwiki'

    results = prepare_mw_rev_score.propagate_by_wbitem(
        df_predictions, df_wbitem, 'prediction',
        source_wikis, preferred_wiki
    ).collect()
    results = [(r.wikiid, r.page_id, r.page_namespace, tuple(r.prediction)) for r in results]

    assert set(results) == expected
    assert len(results) == len(set(results))
