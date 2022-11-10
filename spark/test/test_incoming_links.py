import incoming_links as inclinks
from pyspark.sql import types as T


SCHEMA = T.StructType([
    T.StructField('wiki', T.StringType(), True),
    T.StructField('page_id', T.IntegerType(), True),
    T.StructField('namespace', T.IntegerType(), True),
    T.StructField('namespace_text', T.StringType(), True),
    T.StructField('title', T.StringType(), True),
    T.StructField('incoming_links', T.StringType(), True),
    T.StructField('outgoing_link', T.ArrayType(T.StringType()), True),
])


def test_incoming_to_main_ns(spark):
    df = spark.createDataFrame([
        {
            "wiki": "testwiki",
            "page_id": 1,
            "namespace": 0,
            "namespace_text": "",
            "title": "Main Page",
            "incoming_links": 0,
            "outgoing_link": [],
        },
        # Counts incoming links from other namespaces
        {
            "wiki": "testwiki",
            "page_id": 2,
            "namespace": 1,
            "namespace_text": "Talk",
            "title": "Main Page",
            "incoming_links": 0,
            "outgoing_link": ["Main_Page"],
        },
        # Counts incoming links from same namespace
        {
            "wiki": "testwiki",
            "page_id": 3,
            "namespace": 0,
            "namespace_text": "",
            "title": "Other Page",
            "incoming_links": 0,
            "outgoing_link": ["Main_Page"],
        },
        # Doesn't count incoming links from other wikis
        {
            "wiki": "otherwiki",
            "page_id": 4,
            "namespace": 0,
            "namespace_text": "",
            "title": "Other Page",
            "incoming_links": 0,
            "outgoing_link": ["Main_Page"],
        }
    ], SCHEMA)

    result = inclinks.calc_incoming_links(df).collect()
    assert len(result) == 1, "Only page with incoming links in result"
    row = result[0]
    assert row['page_id'] == 1
    assert row['incoming_links'] == 2


def test_incoming_to_non_main_ns(spark):
    df = spark.createDataFrame([
        {
            "wiki": "testwiki",
            "page_id": 1,
            "namespace": 0,
            "namespace_text": "",
            "title": "Main Page",
            "incoming_links": 0,
            "outgoing_link": ["User_Talk:Someone_(WMF)"],
        },
        # Namespace and title contains spaces
        {
            "wiki": "testwiki",
            "page_id": 2,
            "namespace": 3,
            "namespace_text": "User Talk",
            "title": "Someone (WMF)",
            "incoming_links": 0,
            "outgoing_link": [],
        },
        # Doesn't count unrelated pages
        {
            "wiki": "testwiki",
            "page_id": 3,
            "namespace": 0,
            "namespace_text": "",
            "title": "Example",
            "incoming_links": 0,
            "outgoing_link": ["Other_Page"],
        }
    ], SCHEMA)
    result = inclinks.calc_incoming_links(df).collect()
    assert len(result) == 1, "Only page with incoming links in result"
    row = result[0]
    assert row['page_id'] == 2
    assert row['incoming_links'] == 1
