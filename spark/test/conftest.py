from __future__ import absolute_import
import findspark
findspark.init()  # must happen before importing pyspark

import logging  # noqa: E402
import os  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
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
