from copy import deepcopy
import import_cirrus_indexes
import itertools
import json
import pytest
from typing import Mapping, Sequence
from unittest.mock import MagicMock


def cirrus_cluster():
    return import_cirrus_indexes.CirrusCluster(
        url='http://elasticsearch',
        name='some_cluster',
        replica='q',
        group='a',
    )


@pytest.mark.parametrize('value,expected', [
    ('10b', 10),
    ('4kb', 4 * 2**10),
    ('5mb', 5 * 2**20),
    ('6gb', 6 * 2**30),
    ('1.5tb', 1.5 * 2**40),
    ('invalid', None),
    ('1234', None),
])
def test_to_bytes_by_suffix(value: str, expected: int):
    try:
        result = import_cirrus_indexes.to_bytes_by_suffix(value)
    except Exception:
        assert expected is None
    else:
        assert result == expected


@pytest.mark.parametrize('url,banner,expected', [
    (
        'https://search.svc.eqiad.wmnet:9243',
        {'cluster_name': 'production-search-eqiad'},
        import_cirrus_indexes.CirrusCluster(
            url='https://search.svc.eqiad.wmnet:9243',
            name='production-search-eqiad',
            replica='eqiad',
            group='chi'
        )
    ),
    (
        'https://search.svc.codfw.wmnet:9443',
        {'cluster_name': 'production-search-omega-codfw'},
        import_cirrus_indexes.CirrusCluster(
            url='https://search.svc.codfw.wmnet:9443',
            name='production-search-omega-codfw',
            replica='codfw',
            group='omega'
        )
    ),
    (
        'https://cloudelastic.wikimedia.org:9643',
        {'cluster_name': 'cloudelastic-psi-eqiad'},
        import_cirrus_indexes.CirrusCluster(
            url='https://cloudelastic.wikimedia.org:9643',
            name='cloudelastic-psi-eqiad',
            replica='cloudelastic',
            group='psi'
        )
    ),

])
def test_parse_banner(url, banner, expected):
    result = import_cirrus_indexes.parse_banner(url, banner)
    assert result == expected


def test_parse_cat_aliases():
    result = import_cirrus_indexes.parse_cat_aliases([
        {'alias': 'testwiki_content', 'index': 'testwiki_content_123'},
        {'alias': 'testwiki_titlesuggest', 'index': 'testwiki_titlesuggest_234'},
        {'alias': 'testwiki_file', 'index': 'testwiki_file_345'}
    ])
    assert result == {
        'testwiki_content_123': 'testwiki_content',
        'testwiki_file_345': 'testwiki_file',
    }


@pytest.mark.parametrize('cat_index, expected', [
    (
        {'index': 'testwiki_content_123', 'pri': '1', 'docs.count': '10', 'pri.store.size': '10mb'},
        import_cirrus_indexes.CirrusIndex(
            cluster=cirrus_cluster(),
            url='http://elasticsearch/testwiki_content',
            num_shards=1,
            num_docs=10,
            store_bytes=import_cirrus_indexes.to_bytes_by_suffix('10mb')
        )
    ),
    (
        {'index': 'random_unknown_index', 'pri': '2', 'docs.count': '10', 'pri.store.size': '1tb'},
        None
    )
])
def test_parse_cat_indices(cat_index, expected):
    aliases = {
        'testwiki_content_123': 'testwiki_content',
        'testwiki_general_234': 'testwiki_general',
        'otherwiki_content_345': 'otherwiki_content',
    }
    result = list(import_cirrus_indexes.parse_cat_indices(cirrus_cluster(), [cat_index], aliases))
    if expected is None:
        assert len(result) == 0
    else:
        assert result == [expected]


def test_index_to_data_requests_empty_index():
    index = import_cirrus_indexes.CirrusIndex(
        cluster=cirrus_cluster(),
        url='http://.../something',
        num_shards=1,
        num_docs=0,
        store_bytes=0,
    )
    requests = list(import_cirrus_indexes.index_to_data_requests(index))
    assert requests == []


def test_index_to_data_requests_tiny_index():
    index = import_cirrus_indexes.CirrusIndex(
        cluster=cirrus_cluster(),
        url='http://.../something',
        num_shards=1,
        num_docs=1_000,
        store_bytes=10 * 2**20,  # 10mb
    )
    requests = list(import_cirrus_indexes.index_to_data_requests(index))
    seen = set()
    # tiny index gets one request per shard
    for req in requests:
        assert req.shard_id not in seen
        seen.add(req.shard_id)
        assert req.min_id is None
        assert req.max_id is None
    assert len(seen) == index.num_shards


def assert_non_overlapping_data_requests(all_requests: Sequence[import_cirrus_indexes.DataRequest]):
    # have to sort before grouping
    all_requests = sorted(all_requests, key=lambda x: x.shard_id)
    for shard_id, requests in itertools.groupby(all_requests, key=lambda x: x.shard_id):
        requests = list(requests)
        # If both are None we must have a single request for the shard
        if any(r.min_id is None and r.max_id is None for r in requests):
            assert len(requests) == 1
            continue
        # is not None sorts to beginning
        requests = list(sorted(requests, key=lambda x: (x.min_id is not None, x.min_id)))
        # Must start with None value
        # only first request can have min_id None
        assert requests[0].min_id is None
        assert not any(r.min_id is None for r in requests[1:])
        # Must end with None value
        # only last request can have max_id None
        assert requests[-1].max_id is None, str(requests)
        assert not any(r.max_id is None for r in requests[:-1])
        # there must be no missing values between the requests
        for prev, cur in zip(requests, requests[1:]):
            assert prev.max_id == cur.min_id


def test_index_to_data_requests_multiple_shards():
    index = import_cirrus_indexes.CirrusIndex(
        cluster=cirrus_cluster(),
        url='http://.../something',
        num_shards=32,
        num_docs=100_000_000,
        store_bytes=2**40,  # 1tb
    )
    requests = list(import_cirrus_indexes.index_to_data_requests(index))
    assert_non_overlapping_data_requests(requests)


# From https://docs.python.org/3/library/unittest.mock-examples.html
# search_after mutates a single query object, we need a deepcopy to
# capture it when the call was made.
class CopyingMock(MagicMock):
    def __call__(self, *args, **kwargs):
        args = deepcopy(args)
        kwargs = deepcopy(kwargs)
        return super().__call__(*args, **kwargs)


def test_search_after_no_min_max():
    responses = [
        {'hits': {'hits': [
            {'_id': '1', 'sort': ['1']},
            {'_id': '2', 'sort': ['2']},
        ]}},
        {'hits': {'hits': [
            {'_id': '3', 'sort': ['3']},
            {'_id': '4', 'sort': ['4']},
        ]}},
        {'hits': {'hits': []}}
    ]
    session = CopyingMock()
    session.get().json.side_effect = responses

    req = import_cirrus_indexes.DataRequest(cirrus_cluster(), 'http://...', 0, 10, None, None)
    hits = list(import_cirrus_indexes.search_after(session, req, {}))
    assert len(hits) == 4, 'Returns all expected hits'

    expected_sort = [None, ['2'], ['4']]
    actual_sort = [
        call_args[1]['json'].get('search_after')
        # ignore the first call, that was done when setting side_effect
        for call_args in session.get.call_args_list[1:]]
    assert expected_sort == actual_sort, 'Sort is updated for each query with final hit'


def test_search_after_inclusive_max_id():
    responses = [
        {'hits': {'hits': [
            {'_id': '1', 'sort': ['1']},
            {'_id': '2', 'sort': ['2']},
            {'_id': '3', 'sort': ['3']},
        ]}},
        {'hits': {'hits': []}}
    ]
    session = CopyingMock()
    session.get().json.side_effect = responses

    req = import_cirrus_indexes.DataRequest(cirrus_cluster(), 'http://...', 0, 10, None, '2')
    hits = list(import_cirrus_indexes.search_after(session, req, {}))
    assert len(hits) == 2, 'Returns all expected hits'
    assert hits[-1]['_id'] == '2', 'Stops at requested id'


@pytest.mark.parametrize('message,row,expected', [
    (
        'converts coords into floats',
        {'coordinates': [
            {'coord': {'lat': 1, 'lon': 1}},
        ]},
        {'coordinates': [
            {'coord': {'lat': 1.0, 'lon': 1.0}},
        ]}
    ),
    (
        'drops file_text as array',
        {'file_text': []},
        {}
    ),
    (
        'drops file_text as False',
        {'file_text': False},
        {}
    ),
    (
        'doesnt drop appropriate file_text',
        {'file_text': 'lorem ipsum dolor sit amet'},
        {'file_text': 'lorem ipsum dolor sit amet'}
    ),
    (
        'converts empty labels array into dict',
        {'labels': []},
        {'labels': {}}
    ),
    (
        'converts empty descriptions array into dict',
        {'descriptions': []},
        {'descriptions': {}},
    ),
    (
        'converts single description into array of descriptions',
        {'descriptions': {'pt': 'escritor e comediante britânico'}},
        {'descriptions': {'pt': ['escritor e comediante britânico']}},
    ),
    (
        'doesnt modify array of descriptions',
        {'descriptions': {'pt': ['escritor e comediante britânico']}},
        {'descriptions': {'pt': ['escritor e comediante britânico']}},
    ),
    (
        'drops defaultsort of False',
        {'defaultsort': False},
        {}
    )
])
def test_coerce_types(message: str, row: Mapping, expected: Mapping):
    result = import_cirrus_indexes.coerce_types(row)
    assert result == expected, message


def test_extra_fields_to_json():
    row = {
        'a': 1,
        'b': 2,
        'c': 3,
    }
    result = import_cirrus_indexes.extra_fields_to_json(
        row, dest='out', known_fields=['a', 'c'])
    assert result == {
        'a': 1,
        'c': 3,
        'out': json.dumps({'b': 2}),
    }


def test_partition_per_row(spark):
    rows = ['a', 'b', 'c']
    rdd = import_cirrus_indexes.partition_per_row(spark, rows)
    # converts each partition back to a single row
    output = rdd.mapPartitionsWithIndex(lambda index, iterator: [(index, list(iterator))]).collect()
    assert output == [
        (0, ['a']),
        (1, ['b']),
        (2, ['c']),
    ], 'each row is its own partition'
