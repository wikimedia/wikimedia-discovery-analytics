# Generates and inserts data for the event.mediawiki_revision_score table
#
# Supported pipelines:
#  ores_predictions_weekly
#
# Each gen_* method generates every possible field to ensure compatability. Fields
# are all in schema order. Most fields are either null'd or defaulted to simplify
# the creation of datasets needed.
#

from pyspark.sql import SparkSession, functions as F, types as T
from typing import Mapping, Sequence

from wmf_spark import HivePartitionWriter


def gen_score(
    model_name: str,
    prediction: Sequence[str],
    probability: Mapping[str, float]
) -> Mapping:
    assert len(prediction) > 0, 'parquet requires non-empty arrays'
    return dict(
        model_name=model_name,
        model_version=None,
        prediction=prediction,
        probability=probability,
    )


def gen_row(
    database: str,
    page_id: int,
    scores: Sequence[Mapping],
    page_namespace: int = 0
) -> Mapping:
    return dict(
        _schema=None,
        meta=None,
        database=database,
        performer=None,
        page_id=page_id,
        page_title=None,
        page_namespace=page_namespace,
        page_is_redirect=None,
        rev_id=None,
        rev_parent_id=None,
        rev_timestamp=12345,
        scores={score['model_name']: gen_score(**score) for score in scores},
        errors=None,
        year=None,
        month=None,
        day=None,
        hour=None)


def main(table='event.mediawiki_revision_score'):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.table(table)

    assert df.count() == 0, 'Expecting empty table to populate'

    row_spec = [
        {
            'database': 'testwiki',
            'page_id': 10,
            'page_namespace': 0,
            'scores': [
                {
                    'model_name': 'articletopic',
                    'prediction': ['Culture.Literature'],
                    'probability': {
                        'History and Society.Education': 0.001,
                        'Culture.Literature': 0.998,
                    }
                }
            ]
        },
        {
            'database': 'testwiki',
            'page_id': 11,
            'page_namespace': 0,
            'scores': [
                {
                    'model_name': 'articletopic',
                    'prediction': ['History and Society.Education'],
                    'probability': {
                        'History and Society.Education': 0.998,
                        'Culture.Literature': 0.001,
                    }
                }
            ]
        },
    ]

    rows = [gen_row(**row) for row in row_spec]
    row_schema = df.drop('datacenter', 'year', 'month', 'day', 'hour').schema
    partition = HivePartitionWriter.from_spec(
        '{}/datacenter=eqiad/year=2001/month=1/day=15/hour=19'.format(table))
    partition.overwrite_with(spark.createDataFrame(rows, row_schema))


if __name__ == '__main__':
    main()
