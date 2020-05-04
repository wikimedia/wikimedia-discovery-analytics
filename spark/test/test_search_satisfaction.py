import json

from pyspark.sql import Row

import generate_daily_search_satisfaction as satisfaction


def test_dict_path_happy_path():
    source = {
        'action': 'sleep',
        'metrics': {
            'start': '2038-1-17T12:34Z',
        },
    }

    assert satisfaction.dict_path(source, 'action') == source['action']
    assert satisfaction.dict_path(source, 'action', 'error') is None
    assert satisfaction.dict_path(source, 'action', 'error', default='qqq') == 'qqq'
    assert satisfaction.dict_path(source, 'metrics', 'error') is None
    assert satisfaction.dict_path(source, 'metrics', 'start') == source['metrics']['start']
    assert satisfaction.dict_path(source, 'magic') is None
    assert satisfaction.dict_path(source, 'magic', default='wizards') == 'wizards'


def _fill_fake_events(events, session_id='qwerty'):
    """Expand minimally defined events into full events

    Assists in generating event lists to pass through as_dym_events. Adds
    various metadata that is expected but not necessary to define explicitly.
    """
    default = {
        'action': None,
        'suggestion': None,
        'searchSessionId': session_id,
        'query': None,
        'hitsReturned': None,
        'subTest': None,
        'inputLocation': None,
        'didYouMeanVisible': None,
        'searchToken': None,
        'extraParams': None,
    }
    dt = 0
    for event in events:
        dt += 1
        yield dict(default, dt=dt, **event)


def _tuple_to_row(tuples, struct_type):
    """Convert the plain tuples from as_dym_events into rows we can reference"""
    fields = struct_type.fieldNames()
    for row_data in tuples:
        row = Row(*row_data)
        row.__fields__ = fields
        yield row


def _as_dym_events(fake_events):
    """Helper to run fake events through as_dym_events and work with outputs directly"""
    converted = _tuple_to_row(
        satisfaction.as_dym_events(_fill_fake_events(fake_events)),
        satisfaction.DYM_EVENT_TYPE)
    # The conversion doesn't do any output ordering, spark doesn't care, but we
    # sort by ascending dt here to make tests easier to write.
    return list(sorted(converted, key=lambda evt: evt.dt))


def _extra_params(main='__main__', sugg=None):
    """Helper to build json encoded 'extraParams' value of SearchSatisfaction events"""
    return json.dumps({
        'fallback': {
            'mainResults': {'name': main},
            'querySuggestion': None if sugg is None else {'name': sugg}
        },
    })


def _serp_event(token, **properties):
    return dict({
        'action': 'searchResultPage',
        'searchToken': token,
        'hitsReturned': 5,
        'extraParams': _extra_params(),
    }, **properties)


def test_single_click_event_conversion():
    converted = _as_dym_events([
        _serp_event(token='aaa'),
        {'action': 'click', 'searchToken': 'aaa'}
    ])

    assert len(converted) == 1
    event = converted[0]
    assert event.is_autorewrite_dym is False
    assert event.is_dym is False
    assert event.dym_shown is False
    assert event.dym_clicked is False
    assert event.hits_returned == 5
    assert event.hit_interact is True
    assert event.results_provider == "__main__"
    # No suggestion provided, suggestion provider is not applicable
    assert event.sugg_provider == 'n/a'


def test_multiple_search_event_conversion():
    converted = _as_dym_events([
        _serp_event(token='aaa'),
        _serp_event(token='bbb'),
    ])
    # multiple searches == multiple output events
    assert len(converted) == 2


def test_autorewrite_search_event_conversion():
    converted = _as_dym_events([
        _serp_event(token='aaa', didYouMeanVisible='autorewrite',
                    extraParams=_extra_params('__main__', 'magic_suggestion')),
        {'action': 'click', 'searchToken': 'aaa'},
    ])

    assert len(converted) == 1
    event = converted[0]

    # Query was auto-rewriten
    assert event.is_autorewrite_dym is True
    # Since it was an autorewrite, this search is a suggested search
    assert event.is_dym is True
    # Autorewrite always shows the dym bar
    assert event.dym_shown is True
    # The user did not click the suggested query (it was auto-selected)
    assert event.dym_clicked is False
    assert event.hits_returned == 5
    assert event.hit_interact is True
    assert event.results_provider == "__main__"
    assert event.sugg_provider == 'magic_suggestion'


def test_propagate_suggestion_results_to_source():
    converted = _as_dym_events([
        _serp_event(token='aaa', didYouMeanVisible='yes', suggestion='replacement'),
        _serp_event(token='bbb', query='replacement', inputLocation='dym-suggest')
    ])

    assert len(converted) == 2

    # No autorewrite occured
    assert converted[0].is_autorewrite_dym is False
    # The first search is a user provided search term
    assert converted[0].is_dym is False
    # A suggested search was shown to the user
    assert converted[0].dym_shown is True
    # A search exists in the session for the suggested query
    assert converted[0].dym_clicked is True

    # The second search was not autorewritten
    assert converted[1].is_autorewrite_dym is False
    # The second search is displaying results of a suggested query
    assert converted[1].is_dym is True
    # No dym was shown on the second search
    assert converted[1].dym_shown is False
    # No dym shown also means no dym clicked
    assert converted[1].dym_clicked is False
