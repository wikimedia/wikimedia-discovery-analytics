"""Imports fixtures into spark tables"""

from argparse import ArgumentParser
import json
import logging
from pyspark.sql import SparkSession, types as T
import sys
from typing import Mapping, Optional, Sequence, Tuple

from wmf_spark import HivePartitionWriter


def validate_row(row: Mapping, schema: T.StructType) -> bool:
    """Validate all provided keys exist in schema

    Help ourselves out and fail when the fixture provides columns
    not found in the schema that will be ignored. Reasonable chance
    these are typos or removed fields.
    """
    is_ok = True
    for k, v in row.items():
        try:
            data_type = schema[k]
        except KeyError:
            logging.warning('Unknown field "%s" in fixture', k)
            is_ok = False
            continue
        if isinstance(data_type, T.StructType):
            if isinstance(v, Mapping):
                is_ok &= validate_row(v, data_type)
            else:
                logging.warning('Expected field "%s" in fixture to contain dict', k)
                is_ok = False
    return is_ok


def import_rows(
    spark: SparkSession,
    writer: HivePartitionWriter,
    rows: Sequence[Mapping]
) -> bool:
    """Write rows specified as python dict's to an empty table"""
    df = spark.read.table(writer.table_name)

    if not df._jdf.isEmpty():  # type: ignore
        logging.critical('Table %s is not empty. Refusing to populate', writer.table_name)
        return False

    partition_keys = writer.partition.partition_spec.keys()
    schema = df.drop(*partition_keys).schema
    if not all(validate_row(row, schema) for row in rows):
        return False
    writer.overwrite_with(spark.createDataFrame(rows, schema))  # type: ignore
    return True


def arg_parser() -> ArgumentParser:
    def json_file(path: str):
        with open(path, 'rt') as f:
            return (path, json.load(f))

    parser = ArgumentParser()
    parser.add_argument('configs', nargs='+', type=json_file)
    return parser


def main(
    configs: Sequence[Tuple[str, Mapping]],
    spark: Optional[SparkSession] = None,
) -> int:
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    failed = []
    for path, config in configs:
        logging.info('Importing from %s to %s', path, config.get('partition'))
        try:
            writer = HivePartitionWriter.from_spec(config['partition'])
            if not import_rows(spark, writer, config['rows']):
                failed.append(path)
        except:  # noqa
            logging.exception('Failed importing %s', path)
            failed.append(path)
    logging.info(
        'Imported %d/%d configurations',
        len(configs) - len(failed), len(configs))
    if failed:
        logging.critical('Failed configs: %s', ', '.join(failed))
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main(**dict(vars(arg_parser().parse_args()))))
