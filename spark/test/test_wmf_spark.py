from datetime import datetime, timedelta
from pyspark.sql import functions as F
import pytest
import wmf_spark
from wmf_spark import DtPrecision, HivePartition, HivePartitionTimeRange, HivePartitionWriter


@pytest.mark.parametrize('spec,expect_table_name,expect_partitioning', [
    ('mydb.mytable/key=value', 'mydb.mytable', {'key': 'value'}),
    ('pytest/date=1234/wiki=abc', 'pytest', {'date': '1234', 'wiki': 'abc'}),
    ('qqq/k1=v1/k2=v2/k3=v3', 'qqq', {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}),
    ('full.table/', 'full.table', {}),
    ('full.table', None, None),
])
def test_parse_partition_spec(spec, expect_table_name, expect_partitioning):
    try:
        table_name, partitioning = wmf_spark.parse_partition_spec(spec)
    except ValueError:
        assert expect_table_name is None
    else:
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


def test_HivePartition_partition_cond(spark):
    def test(df, partition, expect):
        cond = partition._partition_cond()
        is_accepted = df.where(cond).count() > 0
        assert expect == is_accepted

    partition = HivePartition('pytest', {'k1': 'v1'})
    unpartitioned = HivePartition('pytest', {})

    df = spark.range(1).withColumn('k1', F.lit('v1'))
    test(df, partition, True)
    test(df, unpartitioned, True)

    df = spark.range(1).withColumn('k1', F.lit('v2'))
    test(df, partition, False)
    test(df, unpartitioned, True)


def test_HivePartitionTimeRange_partition_cond(spark):
    start = datetime(year=2038, month=1, day=17)
    end = datetime(year=2038, month=1, day=18)
    partition = HivePartitionTimeRange(HivePartition('pytest', {}), start, end)

    def test(df, expect):
        cond = partition._partition_cond(df.schema)
        is_accepted = df.where(cond).count() > 0
        assert expect == is_accepted

    def as_daily_df(dt: datetime):
        return (
            spark.range(1)
            .withColumn('year', F.lit(dt.year))
            .withColumn('month', F.lit(dt.month))
            .withColumn('day', F.lit(dt.day))
        )

    def as_hourly_df(dt: datetime):
        return as_daily_df(dt).withColumn('hour', F.lit(dt.hour))

    one_sec = timedelta(seconds=1)

    for as_df in (as_hourly_df, as_daily_df):
        test(as_df(start - one_sec), False)
        test(as_df(start), True)
        test(as_df(start + one_sec), True)

        test(as_df(end - one_sec), True)
        test(as_df(end), False)
        test(as_df(end + one_sec), False)


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


def test_HivePartitionWriter_make_compatible(spark):
    def test(df, expect_schema, expect_error=False):
        writer = HivePartitionWriter.from_spec('db.table/k1=v1')
        writer.partition.schema = lambda _: expect_schema
        try:
            writer.make_compatible(df)
        except Exception as e:
            assert expect_error is True, e
        else:
            assert expect_error is False

    df = spark.range(1)
    expect_schema = df.withColumn('k1', F.lit('v1')).schema
    # Standard happy path
    test(df, expect_schema)
    # Case shouldn't matter
    test(df.withColumn('ID', F.col('id')), expect_schema)
