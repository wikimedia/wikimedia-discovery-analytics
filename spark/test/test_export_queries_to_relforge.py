import pytest

import export_queries_to_relforge


@pytest.fixture
def df_cirrussearch_request_events(get_df_fixture):
    return get_df_fixture('dataframes', 'cirrus_search_request.events')


@pytest.fixture
def df_searchsatisfaction_request_events(get_df_fixture):
    return get_df_fixture('dataframes', 'search_satisfaction.events')


def test_happy_path(df_cirrussearch_request_events, df_searchsatisfaction_request_events):
    result_df = export_queries_to_relforge.extract_from_joined_dfs(df_cirrussearch_request_events,
                                                                   df_searchsatisfaction_request_events)

    assert result_df is not None
    assert set(result_df.columns) == {'dt',
                                      'query',
                                      'hits_total',
                                      'is_bot',
                                      'wiki',
                                      'hits_top1',
                                      'hits_top3',
                                      'hits_top10',
                                      'hits_returned',
                                      'syntax',
                                      'first_page',
                                      'indices'}
    rows = result_df.collect()
    assert len(rows) == 2

    row = rows[0]
    assert row['dt'] == "2021-02-09T09:33:10Z"
    assert row['query'] == "example query"
    assert row['hits_total'] == 30
    assert not row['is_bot']
    assert row['wiki'] == "enwiki"
    assert row['hits_top1'] == "first"
    assert row['hits_top3'] == ["first", "second", "third"]
    assert row['hits_top10'] == ["first", "second", "third", "fourth",
                                 "fifth", "sixth", "seventh", "eighth", "ninth", "tenth"]
    assert row['hits_returned'] == 21
    assert row['syntax'] == ["full_text", "bag_of_words"]
    assert row['first_page']
    assert row['indices'] == ["enwiki_content"]

    row = rows[1]
    assert row['dt'] == "2021-01-09T09:33:10Z"
    assert row['query'] == "example query 2"
    assert row['hits_total'] == 0
    assert not row['is_bot']
    assert row['wiki'] == "enwiki"
    assert row['hits_top1'] is None
    assert row['hits_top3'] == []
    assert row['hits_top10'] == []
    # following information is missing due to unmatched search satisfaction event
    assert row['hits_returned'] is None
    assert row['syntax'] is None
    assert row['first_page'] is None
    assert row['indices'] is None
