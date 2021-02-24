import ores_bulk_ingest


def test_all_pages(mocker):
    api = mocker.MagicMock()
    api.get.return_value = [
        {'query': {'pages': [
            {'pageid': 1, 'ns': 0}
        ]}},
        # Some batches are empty due to miser mode
        {},
        {'query': {'pages': [
            {'pageid': 2, 'ns': 0}
        ]}},
    ]

    pages = list(ores_bulk_ingest.all_pages(api, [0]))
    assert pages == [
        {'pageid': 1, 'ns': 0},
        {'pageid': 2, 'ns': 0},
    ]


def test_score_one_batch(mocker):
    ores = mocker.MagicMock()
    # Usage of iter to return a generator just like normal calls
    ores.score.return_value = iter([
        {
            ores_bulk_ingest.MODEL: {
                'score': {
                    'probability': {
                        'Some.Thing': 0.123,
                    }
                }
            },
        },
        {
            ores_bulk_ingest.MODEL: {
                'error': {},
            }
        }
    ])
    scores = list(ores_bulk_ingest.score_one_batch(ores, 'x', [1, 2], mocker.MagicMock()))
    assert scores == [
        {'Some.Thing': 0.123},
        None
    ]


def test_score_one_batch_retries(mocker):
    ores = mocker.MagicMock()
    ores.score.side_effect = [
        iter([{ores_bulk_ingest.MODEL: {'error': {}}}]),
        iter([
            {
                ores_bulk_ingest.MODEL: {
                    'score': {
                        'probability': {
                            'Some.Thing': 0.123,
                        }
                    }
                }
            }
        ])
    ]
    scores = list(ores_bulk_ingest.score_one_batch(ores, 'x', [1], mocker.MagicMock()))
    assert scores == [
        {'Some.Thing': 0.123}
    ]
