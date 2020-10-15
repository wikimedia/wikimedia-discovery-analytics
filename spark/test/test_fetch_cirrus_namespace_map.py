from dataclasses import dataclass
from pyspark.sql import DataFrame
import pytest

import fetch_cirrussearch_namespace_map


@dataclass
class MockPartition:
    df: DataFrame

    def read(self, spark):
        return self.df

    def schema(self, spark):
        return self.df.schema


class MockPartitionWriter:
    output_df = None

    def overwrite_with(self, df):
        self.output_df = df


@pytest.fixture
def df_canonical_wikis(get_df_fixture):
    return get_df_fixture('dataframes', 'canonical_data.wikis')


def test_happy_path(mocker, df_canonical_wikis):
    # We could mock the responses to http request inside this method, but it
    # seemed more import to keep the test simple.
    fetch_namespaces = mocker.patch('fetch_cirrussearch_namespace_map.fetch_namespaces')
    mock_namespace_responses = {
        'en.wikipedia.org': [(0, 'enwiki_content'), (1, 'enwiki_general')],
        'test.wikipedia.org': [(0, 'testwiki_content'), (1, 'testwiki_general')],
        'commons.wikimedia.org': [(0, 'commonswiki_content'), (1, 'commonswiki_general')],
    }
    fetch_namespaces.side_effect = lambda domain_name: mock_namespace_responses[domain_name]

    canonical_wikis = MockPartition(df_canonical_wikis)
    output_partition = MockPartitionWriter()
    fetch_cirrussearch_namespace_map.main(canonical_wikis, output_partition)

    df = output_partition.output_df
    assert df is not None
    assert set(df.columns) == {'wikiid', 'namespace_id', 'elastic_index'}
    rows = df.collect()
    # 3 wikis, we gave two namespace each
    assert len(rows) == 6
