import json

import fetch_ores_thresholds


def test_get_supported_wikis_response_parsing(get_fixture, mocker):
    # Feed through a recorded response from real api as a happy path test
    response = get_fixture('fetch_ores_thresholds', 'get_supported_wikis.01.json')
    mocker.patch.object(fetch_ores_thresholds, 'requests').get().json.return_value = json.loads(response)

    wikis = fetch_ores_thresholds.get_supported_wikis('articletopic', 'https://ores.pytest/...')
    assert set(wikis) == {'arwiki', 'enwiki', 'kowiki', 'viwiki', 'cswiki'}
    assert len(wikis) == len(set(wikis))


def test_get_labels_response_parsing(get_fixture, mocker):
    response = get_fixture('fetch_ores_thresholds', 'get_labels.01.json')
    mocker.patch.object(fetch_ores_thresholds, 'requests').get().json.return_value = json.loads(response)

    config = fetch_ores_thresholds.Config('arwiki', 'articletopic', 'https://ores.pytest/...')
    labels = fetch_ores_thresholds.get_labels(config)
    # The recorded response was manually cut down to size to make this assertion easier
    assert set(labels) == {
        "Culture.Biography.Biography*",
        "Geography.Geographical",
        "History and Society.Business and economics",
        "STEM.Biology"
    }
    assert len(set(labels)) == len(labels)


def test_threshold_selection_logic(mocker):
    optimizations = {}
    expect = {}

    # The highest threshold will be selected
    expect['a'] = 0.85
    optimizations['a'] = {
        0.7: {'recall': 0.6, 'threshold': 0.85},
        0.5: {'recall': 0.6, 'threshold': 0.7},
    }

    # With no threshold available at 0.7, 0.5 will be queried and returned
    expect['b'] = 0.7
    optimizations['b'] = {
        0.5: {'recall': 0.6, 'threshold': 0.7}
    }

    # With low recall on 0.7 precision, 0.5 will be requested and returned
    expect['c'] = 0.65
    optimizations['c'] = {
        0.7: {'recall': 0.4, 'threshold': 0.80},
        0.5: {'recall': 0.6, 'threshold': 0.65}
    }

    # With no thresholds meeting our precision targets accept only >= 0.9
    expect['d'] = 0.9
    optimizations['d'] = {}

    def get_threshold_at_precision(config, label, target):
        return optimizations[label].get(target, None)

    mocker.patch.object(fetch_ores_thresholds, 'get_threshold_at_precision').side_effect = get_threshold_at_precision
    mocker.patch.object(fetch_ores_thresholds, 'get_labels').return_value = list(expect.keys())
    mocker.patch.object(fetch_ores_thresholds, 'get_supported_wikis').return_value = ['pytestwiki']

    thresholds = fetch_ores_thresholds.get_all_thresholds('pytestmodel', 'https://ores.pytest/v3/scores')
    assert thresholds == {'pytestwiki': expect}
