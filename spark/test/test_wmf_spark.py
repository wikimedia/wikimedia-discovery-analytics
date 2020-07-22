from datetime import datetime
from pyspark.sql import functions as F
import pytest
import wmf_spark
from wmf_spark import DtPrecision


@pytest.mark.parametrize('spec,expect_table_name,expect_partitioning', [
    ('mydb.mytable/key=value', 'mydb.mytable', {'key': 'value'}),
    ('pytest/date=1234/wiki=abc', 'pytest', {'date': '1234', 'wiki': 'abc'}),
    ('qqq/k1=v1/k2=v2/k3=v3', 'qqq', {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})
])
def test_parse_partition_spec(spec, expect_table_name, expect_partitioning):
    table_name, partitioning = wmf_spark.parse_partition_spec(spec)
    assert table_name == expect_table_name
    assert partitioning == expect_partitioning


@pytest.mark.parametrize('spec,expect_start,expect_end', [
    (
        'db.table/k1=v1@2038-01-17/2038-01-18',
        datetime(year=2038, month=1, day=17),
        datetime(year=2038, month=1, day=18)
    ),
    (
        'db.table/@2038-01-17 01:00:00/2038-01-17 03:00:00',
        datetime(year=2038, month=1, day=17, hour=1),
        datetime(year=2038, month=1, day=17, hour=3)
    )
])
def test_partition_time_range(spec, expect_start, expect_end):
    partition_spec, dt_start, dt_end = \
        wmf_spark.parse_partition_range_spec(spec)
    assert dt_start == expect_start
    assert dt_end == expect_end


def test_row_datetime_ts_happy_path(spark):
    def test(df, expect):
        col = wmf_spark.row_datetime_ts(df.schema)
        value = df.select(col.alias('value')).head().value
        assert value == expect.timestamp()

    # by date
    df = (
        spark.range(1)
        .withColumn('date', F.lit('20380117'))
    )
    test(df, datetime(year=2038, month=1, day=17))

    # by y/m/d
    df = (
        spark.range(1)
        .withColumn('year', F.lit(2038))
        .withColumn('month', F.lit(1))
        .withColumn('day', F.lit(17))
    )
    test(df, datetime(year=2038, month=1, day=17))

    # by y/m/d/h
    df = df.withColumn('hour', F.lit(3))
    test(df, datetime(year=2038, month=1, day=17, hour=3))


@pytest.mark.parametrize('dt,expect', [
    (datetime(year=2038, month=1, day=1), DtPrecision.DAY),
    (datetime(year=2038, month=1, day=1, hour=0), DtPrecision.DAY),
    (datetime(year=2038, month=1, day=1, hour=2), DtPrecision.HOUR),
    (datetime(year=2038, month=1, day=1, hour=2, minute=4), DtPrecision.INVALID),
])
def test_DtPrecision_of(dt, expect):
    assert DtPrecision.of(dt) == expect


def test_DtPrecision_min_value():
    # Assert which value holds the smallest time range
    assert min(DtPrecision.HOUR, DtPrecision.DAY) == DtPrecision.HOUR
    assert min(DtPrecision.INVALID, DtPrecision.DAY) == DtPrecision.INVALID
    assert min(DtPrecision.HOUR, DtPrecision.INVALID) == DtPrecision.INVALID


def test_HivePartitionWriter_make_compatible():
    pass
