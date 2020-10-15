from __future__ import absolute_import
import findspark
findspark.init()  # must happen before importing pyspark

import json  # noqa: E402
import logging  # noqa: E402
import os  # noqa: E402
from pyspark.sql import SparkSession, types as T  # noqa: E402
import pytest  # noqa: E402


def quiet_log4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a spark context.

    Args:
        request: pytest.FixtureRequest object

    Returns:
        SparkContext for tests
    """

    quiet_log4j()

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pyspark-local-testing")
        # By default spark will shuffle to 200 partitions, which is
        # way too many for our small test cases. This cuts execution
        # time of the tests in half.
        .config('spark.sql.shuffle.partitions', 4)
    )
    if 'XDG_CACHE_HOME' in os.environ:
        builder.config('spark.jars.ivy', os.path.join(os.environ['XDG_CACHE_HOME'], 'ivy2'))

    with builder.getOrCreate() as spark:
        yield spark


@pytest.fixture(scope="session")
def spark_context(spark):
    return spark.sparkContext


@pytest.fixture
def fixture_dir():
    return os.path.join(os.path.dirname(__file__), 'fixtures')


@pytest.fixture
def get_fixture(fixture_dir):
    def fn(group, name):
        with open(os.path.join(fixture_dir, group, name), 'rt') as f:
            return f.read()

    return fn


def create_df_fixture(df):
    """Create a fixture from a dataframe

    Not actually used in the test suite, but included as documentation for how
    to create the files used by get_df_fixture. Don't put real PII in fixtures.
    """
    return {
        # This returns a string, but we will need the decoded
        # structure when loading so no reason to double encode.
        'schema': json.loads(df.schema.json()),
        # Turn everything into a plain list of rows and values
        'rows': [[getattr(row, field) for field in df.columns] for row in df.collect()],
    }


@pytest.fixture
def get_df_fixture(spark, get_fixture):
    def fn(group, name):
        raw_data = json.loads(get_fixture(group, name + '.json'))
        schema = T.StructType.fromJson(raw_data['schema'])
        return spark.createDataFrame(raw_data['rows'], schema)
    return fn
