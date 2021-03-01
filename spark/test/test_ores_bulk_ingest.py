import ores_bulk_ingest
import pytest


@pytest.fixture
def mock_api(mocker):
    api = mocker.MagicMock()
    api.get.return_value = [
        {'query': {'pages': [
            {'pageid': 1, 'ns': 0, 'revisions': [{'revid': 101}]},
        ]}},
        # Some batches are empty due to miser mode
        {},
        {'query': {'pages': [
            # We've seen responses with no revisions key for unknown reasons
            {'pageid': 2, 'ns': 0},
            {'pageid': 3, 'ns': 0, 'revisions': [{'revid': 103}]},
        ]}},
    ]
    return api


def test_all_pages(mock_api):
    pages = list(ores_bulk_ingest.all_pages(mock_api, [0]))
    assert pages == [
        {'pageid': 1, 'ns': 0, 'revisions': [{'revid': 101}]},
        {'pageid': 2, 'ns': 0},
        {'pageid': 3, 'ns': 0, 'revisions': [{'revid': 103}]},
    ]


# raw responses
ores_rev_resp_good = {
    ores_bulk_ingest.MODEL: {
        'score': {
            'probability': {
                'Some.Thing': 0.123,
            }
        }
    },
}

ores_rev_resp_bad = {
    ores_bulk_ingest.MODEL: {
        'error': {},
    }
}

# Expected output after processing ores api response
ores_prediction = {'Some.Thing': 0.123}


@pytest.mark.parametrize('responses,expect', [
    [
        # An individual revision failing must return None as prediction
        [[ores_rev_resp_good, ores_rev_resp_bad]],
        [ores_prediction, None],
    ],
    [
        [
            # A request for multiple revid's returning a single error
            # must be retried and result returned.
            [ores_rev_resp_bad],
            [ores_rev_resp_good, ores_rev_resp_good]
        ],
        [ores_prediction, ores_prediction],
    ],
])
def test_score_one_batch(mocker, expect, responses):
    ores = mocker.MagicMock()
    # Usage of iter to return a generator just like normal calls
    ores.score.side_effect = [iter(x) for x in responses]
    scores = list(ores_bulk_ingest.score_one_batch(ores, 'x', [1, 2], mocker.MagicMock(), sleep=lambda x: None))
    assert scores == expect


def test_fetch_scores(mocker, mock_api):
    # fetch_scores composes everything together. Ensure it composes
    # somewhat sanely.
    ores = mocker.MagicMock()
    ores.score.side_effect = [iter([
        # mock api returns 3 revisions, but only 2 are valid and
        # will be requested.
        ores_rev_resp_good, ores_rev_resp_good,
    ])]
    result = list(ores_bulk_ingest._fetch_scores(
        mock_api, ores, 'pytestwiki', [0], mocker.MagicMock()
    ))
    assert result == [
        # page_id, namespace, prediction
        (1, 0, ores_prediction),
        (3, 0, ores_prediction),
    ]
