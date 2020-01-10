from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.sensors.hive_partition_range_sensor_plugin import HivePartitionRangeSensor


HQL = """
INSERT OVERWRITE TABLE discovery.popularity_score
    PARTITION(
        agg_days=7,
        year={{ execution_date.year }},
        month={{ execution_date.month }},
        day={{ execution_date.day }})
    SELECT
        hourly.project,
        hourly.page_id,
        SUM(hourly.view_count) / agg.view_count AS score
    FROM
        wmf.pageview_hourly hourly
    JOIN (
        SELECT
            project,
            SUM(view_count) AS view_count
        FROM
            wmf.pageview_hourly
        WHERE
            page_id IS NOT NULL
            AND TO_DATE(CONCAT_WS('-',
                    CAST(year AS string), CAST(month AS string),
                    CAST(day AS string)))
                BETWEEN TO_DATE('{{ ds }}') AND DATE_ADD(TO_DATE('{{ ds }}'), 7)
        GROUP BY
            project
        ) agg on hourly.project = agg.project
    WHERE
        hourly.page_id IS NOT NULL
        AND TO_DATE(CONCAT_WS('-',
                CAST(year AS string), CAST(month AS string),
                CAST(day AS string)))
            BETWEEN TO_DATE('{{ ds }}') AND DATE_ADD(TO_DATE('{{ ds }}'), 7)
    GROUP BY
        hourly.project,
        hourly.page_id,
        agg.view_count
"""

# Default kwargs for all Operators
default_args = {
    'owner': 'discovery-analytics',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 8),
    'email': ['discovery-alerts@lists.wikimedia.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}


with DAG(
    'popularity_score_weekly',
    default_args=default_args,
    # Once a week at midnight on Sunday morning
    schedule_interval='0 0 * * 0',
    # As a weekly job there should never really be more than
    # one running at a time.
    max_active_runs=1,
    catchup=False,
) as dag:
    # Require hourly partitions to exist before running
    wait_for_pageview_hourly = HivePartitionRangeSensor(
        task_id='wait_for_pageview_hourly',
        # We send a failure email once a day when the expected data is not
        # found. Since this is a weekly job we wait up to 4 days for the data
        # to show up before giving up and waiting for next scheduled run.
        timeout=60 * 60 * 24,  # 24 hours
        retries=4,
        email_on_retry=True,
        # partition range selection
        table='wmf.pageview_hourly',
        period=timedelta(days=7),
        partition_frequency='hours',
        partition_specs=[
            [('year', None), ('month', None), ('day', None), ('hour', None)]
        ])

    # Generate the data
    popularity_score = HiveOperator(
        task_id='popularity_score',
        hiveconfs={
            # When not specified hive has previously emitted
            # hundreds of tiny partitions. As of jan 2020
            # 6 partitions results in ~100MB per partition
            'mapred.reduce.tasks': 6,
        },
        hql=HQL)
    wait_for_pageview_hourly >> popularity_score

    # Simplify external task sensor by having a common
    # 'complete' node at end of all dags
    complete = DummyOperator(task_id='complete')
    popularity_score >> complete
