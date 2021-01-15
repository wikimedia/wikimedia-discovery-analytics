"""Various utility functions for spark scripts

Includes abstractions for reading/writing data to hive tables. Each of these
is intended to remove concerns about exact partitioning, or ranges of partitions
to read, away from the script and into the caller. Each class has a `from_spec`
classmethod intended for use with ArgumentParser that takes a string and
returns an instance of the class ready to read or write data.

While everything refers to partitions, as that is our typical goal, nothing
explicitly guarantees the referenced k/v pairs of the partition are actually
partitioning keys, or that all partitioning keys have been specified. This is
intentional to keep things flexible. Writing still guarantees all columns of
the destination table, including partitioning keys, are available.
"""
from __future__ import annotations
from datetime import datetime
from enum import Enum
from functools import reduce
from pyspark.sql import (
    Column, DataFrame, SparkSession, SQLContext, WindowSpec,
    functions as F, types as T)
from typing import Mapping, Tuple, Union


def limit_top_n(df: DataFrame, window: WindowSpec, n: int) -> DataFrame:
    """Limit each window to the top n values"""
    assert '_row_num' not in df.columns
    return (
        df
        .withColumn('_row_num', F.row_number().over(window))
        .where(F.col('_row_num') < n + 1)  # row_number is 1-indexed
        .drop('_row_num')
    )


def parse_partition_spec(spec: str) -> Tuple[str, Mapping[str, str]]:
    """Parse a hive partition specification

    Parses strings in the format hive uses to report partitions. One small
    extension to this format is the requirement to suffix a table name with `/`
    to not specify partitioning. This suffix signifys intent, and helps ensure
    we actually want to read the whole table and didn't simply forget to add
    the partitioning information.

    Examples:
        db.table/k1=v1/k2=v2
        db.table/
    """
    if '/' not in spec:
        raise ValueError('No partition delimiter found')
    # Strip trailing /, it would leave an empty string as a piece
    pieces = spec.rstrip('/').split('/')
    table_name = pieces[0]
    partitions = dict(kv_pair.split('=', 1) for kv_pair in pieces[1:])  # type: ignore
    return table_name, partitions


def parse_partition_range_spec(
    spec: str,
) -> Tuple[str, datetime, datetime]:
    """Parse a partition specification augmented with a time range

    Extends the format used by parse_partition_spec to include a time range
    specification. Timestamps must specify either dates or hours, arbitrary
    time is not supported. Only the time range is parsed here, the prefixed
    partition_spec is returned to the caller, likely it will pass it through
    HivePartition.from_spec.

    Examples:
        db.table/k1=v1/k2=v2@2020-01-01/2020-01-02
        db.table/k1=v1@2020-01-01 12:00:00/2020-01-01 13:00:00
    """
    partition_spec, range_spec = spec.split('@')
    for dt_format in ('%Y-%m-%d', '%Y-%m-%d %H:00:00'):
        try:
            dt_start, dt_end = [
                datetime.strptime(dt, dt_format)
                for dt in range_spec.split('/')
            ]
            break
        except ValueError:
            pass
    else:
        raise ValueError('Unrecognized timestamps')
    return partition_spec, dt_start, dt_end


def row_datetime_ts(schema: T.StructType) -> Column:
    """Integer column representing unix timestamp of partitioning

    Hardcodes various assumptions about how dated partitions are handled at
    wmf.
    """
    def has_cols(*required: str) -> bool:
        return all(col in schema.names for col in required)

    if has_cols('year', 'month', 'day', 'hour'):
        row_datetime_str = F.concat(
            F.col('year'), F.lit('-'),
            F.lpad(F.col('month'), 2, '0'), F.lit('-'),
            F.lpad(F.col('day'), 2, '0'), F.lit(' '),
            F.lpad(F.col('hour'), 2, '0'), F.lit(':00:00'))
    elif has_cols('year', 'month', 'day'):
        row_datetime_str = F.concat(
            F.col('year'), F.lit('-'),
            F.lpad(F.col('month'), 2, '0'), F.lit('-'),
            F.lpad(F.col('day'), 2, '0'),
            F.lit(' 00:00:00'))
    elif has_cols('date'):
        # We can't know (well, without looking at some live partitions and guessing),
        # if date is formatted yyyy-mm-dd, yyyymmdd, or whatever else. Current convention
        # is yyyymmdd so hardcoding that.
        date = F.col('date')
        year = F.substring(date, 1, 4)
        month = F.substring(date, 5, 2)
        day = F.substring(date, 7, 2)
        row_datetime_str = F.concat(
            year, F.lit('-'), month, F.lit('-'), day,
            F.lit(' 00:00:00'))
    else:
        raise ValueError('schema does not match any known datetime partitioning')

    return F.unix_timestamp(row_datetime_str, 'yyyy-MM-dd HH:mm:ss')


class HivePartition:
    """Specifys a set of data to load from a spark table

    Provides support for reading a single partition or an unpartitioned table.
    Specifications are in the same format hive uses when reporting individual
    partitions. The unpartitioned use case is a small extension to the hive
    format. Reading all partitions of a table requires the single backslash to
    signify intent.

    Specific partition: db.table/k1=v1/k2=v2
    Unpartitioned (or all partitions): db.table/

    While we say singular partition, that is only by convention. It can just as
    easily load multiple partitions.  This is mostly an abstraction for
    specifying a subset of a table using constant filtering values from a
    single string (typically, cli arg).
    """
    def __init__(self, table_name: str, partition_spec: Mapping[str, str]):
        self.table_name = table_name
        # TODO: naming, this is the k/v pairs that uniquely identify the partition
        # within the table
        self.partition_spec = partition_spec

    @classmethod
    def from_spec(cls, spec: str) -> HivePartition:
        table_name, partition_spec = parse_partition_spec(spec)
        if partition_spec is None:
            raise ValueError('No partition information in spec:' + spec)
        return cls(table_name, partition_spec)

    def schema(self, spark: Union[SQLContext, SparkSession]) -> T.StructType:
        return spark.read.table(self.table_name).schema

    def _partition_cond(self) -> Column:
        return reduce(lambda a, b: a & b, (
            F.col(k) == v for k, v in self.partition_spec.items()
        ), F.lit(True))

    def read(self, spark: Union[SQLContext, SparkSession]) -> DataFrame:
        return spark.read.table(self.table_name).where(self._partition_cond())


class DtPrecision(Enum):
    # The associated numerical value refers to the size of
    # the time range. An hour is smaller than a day. An invalid
    # range covers no time.
    INVALID = 0
    HOUR = 1
    DAY = 2

    def __lt__(self, other):
        return self.value < other.value

    @classmethod
    def of(cls, dt: datetime) -> DtPrecision:
        """Classify the specificity of a datetime as hourly or daily"""
        if dt.microsecond != 0 or dt.second != 0 or dt.minute != 0:
            return cls.INVALID
        return cls.DAY if dt.hour == 0 else cls.HOUR


class HivePartitionTimeRange():
    """Allows specifying a range of time based partitions

    Provides additional support for ranges of time based partitions. This is
    specific to how time based partitions are handled at wmf. The `from_spec`
    method falls back to HivePartition when no time range delimiter (@) is
    provided, making it an extension to the initial spec.

    Example daily: db.table/k1=v1@2038-01-17/2038-01-18
    Example hourly: db.table/@2038-01-17 01:00:00/2038-01-17 02:00:00
    """

    def __init__(self, partition: HivePartition, start: datetime, end: datetime):
        self.partition = partition
        self.start = start
        self.end = end
        self._dt_precision = min(DtPrecision.of(start), DtPrecision.of(end))
        if self._dt_precision == DtPrecision.INVALID:
            raise ValueError('only hourly or daily partition ranges are supported')

    @classmethod
    def from_spec(cls, spec: str):
        if '@' in spec:
            partition_spec, dt_start, dt_end = parse_partition_range_spec(spec)
            return cls(HivePartition.from_spec(partition_spec), dt_start, dt_end)
        else:
            # duck typing. Conceptually this is the time range "all"
            return HivePartition.from_spec(spec)

    def schema(self, spark: Union[SQLContext, SparkSession]) -> T.StructType:
        return self.partition.schema(spark)

    def _partition_cond(self, schema: T.StructType) -> Column:
        """Boolean column selecting specified partitions"""
        # Builds in a wmf assumption about how partitioning is done
        if self._dt_precision == DtPrecision.HOUR and 'hour' not in schema.names:
            raise Exception('Cannot select hourly range from daily partitioned table')
        row_datetime = row_datetime_ts(schema)
        start_cond = row_datetime >= self.start.timestamp()
        end_cond = row_datetime < self.end.timestamp()
        return start_cond & end_cond

    def read(self, spark: Union[SQLContext, SparkSession]) -> DataFrame:
        df = self.partition.read(spark)
        return df.where(self._partition_cond(df.schema))


class HivePartitionWriter:
    """Writes a DataFrame to a specific hive partition.

    Abstracts away the details of writing a dataframe to hive in the wmf hadoop
    installation. The `from_spec` method uses the same format as HivePartition,
    the class itself is a wrapper around HivePartition. By separating from
    reading this allows specifying the intent of writing in function
    signatures.
    """
    def __init__(self, partition: HivePartition):
        self.partition = partition

    @classmethod
    def from_spec(cls, spec: str) -> HivePartitionWriter:
        return cls(HivePartition.from_spec(spec))

    def make_compatible(self, df: DataFrame) -> DataFrame:
        """Align dataframe to the target schema

        Takes care of getting everything in order so DataFrame.insertInto
        does what we want. Currently only validates the top level set of
        column names matches.

        TODO: recursive schema compatability validation. See mjolnir.
        """
        for k, v in self.partition.partition_spec.items():
            if k in df.columns:
                raise Exception('Partition key {} overwriting dataframe column'.format(k))
            df = df.withColumn(k, F.lit(v))

        def schema_names(schema):
            return {x.lower() for x in schema.names}

        expect_schema = self.partition.schema(df.sql_ctx)
        # The .select() call below would throw, but provide a cryptic error message
        if schema_names(df.schema) != schema_names(expect_schema):
            raise ValueError('Schemas do not have matching names: {} != {}'.format(
                df.schema, expect_schema))

        # In testing with insertInto spark put values in the wrong columns of the
        # table unless we made the order exactly the same.
        # TODO: This doesn't handle type compatability, this is a problem both for
        # int/long mixups, and for StructType.
        return df.select(*expect_schema.names)

    def overwrite_with(self, df: DataFrame) -> None:
        """Overwrite the partition with the DataFrame"""
        df = self.make_compatible(df)
        # Required for insert_into to only overwrite the partitions being
        # written to and not the whole table
        df.sql_ctx.setConf('spark.sql.sources.partitionOverwriteMode', 'dynamic')
        # Nonstrict is required to allow partition selection on per-row basis. We
        # can't specify anything more strict from spark.
        df.sql_ctx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        # Note that .mode('overwrite') would be ignored, the insertInto kwarg and its
        # default value of False take precedence.
        df.write.insertInto(self.partition.table_name, overwrite=True)
