""" Subgraph And Query Metrics Init Dag
      Create table and partitions for subgraph_metrics_weekly dag and subgraph_query_metrics_daily
      dag
    Subgraph Metrics Weekly Dag
      Collects aggregate metrics about wikidata subgraphs and saves into a table.
      Also collects per-subgraph and subgraph-pair metrics about wikidata subgraphs
      and saves into tables.
      Runs every monday to match with the wikidata dumps and other input data.
    Subgraph Query Metrics Daily Dag
      Extracts general query metrics, subgraph query metrics, per-subgraph query
      metrics, and subgraph-pair query metrics and saves each in tables.
      Runs daily.
"""

from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from wmf_airflow import DAG
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import (DagConf, wmf_conf, YMD_PARTITION, WDQS_SPARK_TOOLS,
                                  eventgate_partitions)

dag_conf = DagConf('subgraph_and_query_metrics_conf')

WIKIDATA_TABLE = dag_conf('wikidata_dumps_table')
ALL_SUBGRAPHS_TABLE = dag_conf('all_subgraphs_table')
TOP_SUBGRAPH_ITEMS_TABLE = dag_conf('top_subgraph_items_table')
TOP_SUBGRAPH_TRIPLES_TABLE = dag_conf('top_subgraph_triples_table')
EVENT_SPARQL_QUERY_TABLE = dag_conf("event_sparql_query_table")
PROCESSED_QUERY_TABLE = dag_conf('processed_query_table')
SUBGRAPH_QITEM_MATCH_TABLE = dag_conf('subgraph_qitem_match_table')
SUBGRAPH_PREDICATE_MATCH_TABLE = dag_conf('subgraph_predicate_match_table')
SUBGRAPH_URI_MATCH_TABLE = dag_conf('subgraph_uri_match_table')
SUBGRAPH_QUERY_MAPPING_TABLE = dag_conf('subgraph_query_mapping_table')
GENERAL_SUBGRAPH_METRICS_TABLE = dag_conf('general_subgraph_metrics_table')
PER_SUBGRAPH_METRICS_TABLE = dag_conf('per_subgraph_metrics_table')
SUBGRAPH_PAIR_METRICS_TABLE = dag_conf('subgraph_pair_metrics_table')
GENERAL_QUERY_METRICS_TABLE = dag_conf('general_query_metrics_table')
GENERAL_SUBGRAPH_QUERY_METRICS_TABLE = dag_conf('general_subgraph_query_metrics_table')
PER_SUBGRAPH_QUERY_METRICS_TABLE = dag_conf('per_subgraph_query_metrics_table')
SUBGRAPH_PAIR_QUERY_METRICS_TABLE = dag_conf('subgraph_pair_query_metrics_table')
MIN_ITEMS = dag_conf('min_items')
TOP_N = dag_conf('top_n')
FILTERING_LIMIT = dag_conf('filtering_limit')
WIKI = dag_conf('wiki')

DATE = "{{ execution_date.format('%Y%m%d') }}"

# Default kwargs for all Operators
default_args = {
    'start_date': datetime(2022, 6, 20),
}

with DAG(
        'subgraph_and_query_metrics_init',
        default_args=default_args,
        schedule_interval='@once',
        user_defined_macros={
            'dag_conf': dag_conf.macro,
            'wmf_conf': wmf_conf.macro,
        },
) as subgraph_and_query_metrics_dag_init:
    complete = DummyOperator(task_id='complete')
    HiveOperator(
        task_id='create_tables',
        hql="""
            CREATE TABLE IF NOT EXISTS {{ dag_conf.general_subgraph_metrics_table }} (
                `total_items`                        bigint  COMMENT 'Total number of items in Wikidata',
                `total_triples`                      bigint  COMMENT 'Total number of triples in Wikidata',
                `percent_subgraph_item`              double  COMMENT 'Percentage of items covered by the top subgraphs',
                `percent_subgraph_triples`           double  COMMENT 'Percentage of triples covered by the top subgraphs',
                `num_subgraph`                       bigint  COMMENT 'Number of subgraphs in wikidata (using groups of P31)',
                `num_top_subgraph`                   bigint  COMMENT 'Number of top subgraphs (has at least `minItems` items)',
                `subgraph_size_percentiles`          array<double>  COMMENT 'List of values containing the percentile values from 0.1 to 0.9 of the size of subgraphs (in triples)',
                `subgraph_size_mean`                 double  COMMENT 'Mean of the size (in triples) of all subgraphs'
            )
            PARTITIONED BY (
                `snapshot` string,
                `wiki` string
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_general_subgraph_metrics_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.per_subgraph_metrics_table }} (
                `subgraph`                          string  COMMENT 'URI of the subgraphs in wikidata',
                `item_count`                        bigint  COMMENT 'Total number of items/entities in subgraph',
                `triple_count`                      bigint  COMMENT 'Total number of triples in subgraph',
                `predicate_count`                   bigint  COMMENT 'Total number of distinct predicates in subgraph',
                `item_percent`                      double  COMMENT 'Percent of items/entities in subgraph compared to total items in Wikidata',
                `triple_percent`                    double  COMMENT 'Percent of triples in subgraph compared to total triples in Wikidata',
                `density`                           double  COMMENT 'Average triples per item, represents density of subgraphs',
                `item_rank`                         bigint  COMMENT 'Rank of the subgraph by number of items it contains in descending order',
                `triple_rank`                       bigint  COMMENT 'Rank of the subgraph by number of triples it contains in descending order',
                `triples_per_item_percentiles`      array<double>  COMMENT 'List of 0.1 to 0.9 percentile of triples per item in each subgraph',
                `triples_per_item_mean`             double  COMMENT 'Mean of triples per item in each subgraph',
                `num_direct_triples`                bigint  COMMENT 'Number of direct triples (triples that are not statements)',
                `num_statements`                    bigint  COMMENT 'Number of statements (wikidata.org/prop/)',
                `num_statement_triples`             bigint  COMMENT 'Number of triples in the full statements (everything within the statements)',
                `predicate_counts`                  map<string, bigint>  COMMENT 'Map of predicates and the number of its occurrences in the subgraph',
                `subgraph_to_WD_triples`            bigint  COMMENT 'Number of triples connecting this subgraph to other subgraphs',
                `WD_to_subgraph_triples`            bigint  COMMENT 'Number of triples connecting other subgraphs to this subgraph'
            )
            PARTITIONED BY (
                `snapshot` string,
                `wiki` string
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_per_subgraph_metrics_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.subgraph_pair_metrics_table }} (
                `subgraph1`                                string  COMMENT 'First subgraph of the subgraph pair',
                `subgraph2`                                string  COMMENT 'Second subgraph of the subgraph pair',
                `triples_from_1_to_2`                      bigint  COMMENT 'Number of directed triples that connect from subgraph1 to subgraph2',
                `triples_from_2_to_1`                      bigint  COMMENT 'Number of directed triples that connect from subgraph2 to subgraph1',
                `common_predicate_count`                   bigint  COMMENT 'Number of predicates found in both subgraphs',
                `common_item_count`                        bigint  COMMENT 'Number of items found in both subgraphs',
                `common_item_percent_of_subgraph1_items`   double  COMMENT 'Percent of common items w.r.t total items in subgraph1',
                `common_item_percent_of_subgraph2_items`   double  COMMENT 'Percent of common items w.r.t total items in subgraph2'
            )
            PARTITIONED BY (
                `snapshot` string,
                `wiki` string
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_subgraph_pair_metrics_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.general_query_metrics_table }} (
                `total_query_count`                 bigint  COMMENT 'Total number of queries',
                `processed_query_count`             bigint  COMMENT 'Total number of queries that were parsed/processed successfully',
                `percent_processed_query`           double  COMMENT 'Percentage of queries that were parsed successfully (w.r.t 200 and 500 queries, except monitoring queries)',
                `distinct_query_count`              bigint  COMMENT 'Number of distinct processed queries',
                `percent_query_repeated`            double  COMMENT 'Percentage of query repeated',
                `total_ua_count`                    bigint  COMMENT 'Total distinct user-agents (from UA string)',
                `total_time`                        bigint  COMMENT 'Total time in milliseconds taken to run all the processed queries',
                `status_code_query_count`           map<bigint, bigint>  COMMENT 'Number of queries per status code',
                `query_time_class_query_count`      map<string, bigint>  COMMENT 'Number of queries per query time class'
            )
            PARTITIONED BY (
                `year`              int     COMMENT 'Unpadded year of queries',
                `month`             int     COMMENT 'Unpadded month of queries',
                `day`               int     COMMENT 'Unpadded day of queries',
                `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_general_query_metrics_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.general_subgraph_query_metrics_table }} (
                `total_subgraph_query_count`        bigint  COMMENT 'Number of queries that access the top subgraphs',
                `ua_subgraph_dist`                  array<struct<subgraph_count:bigint, ua_count:bigint> >  COMMENT 'List of the number of user-agents accessing how many subgraphs each',
                `query_subgraph_dist`               array<struct<subgraph_count:bigint, query_count:bigint> >  COMMENT 'List of the number of queries accessing how many subgraphs at once',
                `query_time_class_subgraph_dist`    array<
                                                        struct<
                                                           subgraph_count: string,
                                                           query_time_class: map<string, bigint>
                                                        >
                                                    >  COMMENT 'Query time class distribution of queries that access 1,2,3,4,4+,n/a subgraphs. Here `n/a` means a query does not access any of the top subgraphs'
            )
            PARTITIONED BY (
                `year`              int     COMMENT 'Unpadded year of queries',
                `month`             int     COMMENT 'Unpadded month of queries',
                `day`               int     COMMENT 'Unpadded day of queries',
                `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_general_subgraph_query_metrics_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.per_subgraph_query_metrics_table }} (
                `subgraph`                             string  COMMENT 'URI of the subgraphs in wikidata',
                `query_count`                          bigint  COMMENT 'Number of queries accessing this subgraph',
                `query_time`                           bigint  COMMENT 'Total time of queries accessing this subgraph',
                `ua_count`                             bigint  COMMENT 'Number of distinct user agents accessing this subgraph',
                `query_type`                           bigint  COMMENT 'Distinct types of queries in this subgraph (rough estimate from operator list)',
                `percent_query_count`                  double  COMMENT 'Percent queries in this subgraph w.r.t total parsed queries',
                `percent_query_time`                   double  COMMENT 'Percent total query time in this subgraph w.r.t total parsed query time',
                `percent_ua_count`                     double  COMMENT 'Percent unique user-agents w.r.t total user-agents of parsed queries',
                `query_count_rank`                     integer COMMENT 'Rank of the subgraph in terms of number of query in descending order',
                `query_time_rank`                      integer COMMENT 'Rank of the subgraph in terms of query time in descending order',
                `avg_query_time`                       double  COMMENT 'Average time(ms) per query in this subgraph',
                `qid_count`                            bigint  COMMENT 'Number of queries that matched to this subgraph due to the subgraph Qid match',
                `item_count`                           bigint  COMMENT 'Number of queries that matched to this subgraph due to the subgraph items Qid match',
                `pred_count`                           bigint  COMMENT 'Number of queries that matched to this subgraph due to predicate match',
                `uri_count`                            bigint  COMMENT 'Number of queries that matched to this subgraph due to URI match',
                `literal_count`                        bigint  COMMENT 'Number of queries that matched to this subgraph due to literals match',
                `query_time_class_counts`              map<string, bigint>  COMMENT 'Number of queries per query time class',
                `ua_info`                              array<
                                                           struct<
                                                                ua_rank: integer,
                                                                ua_query_count: bigint,
                                                                ua_query_time: bigint,
                                                                ua_query_type: bigint,
                                                                ua_query_percent: double,
                                                                ua_query_time_percent: double,
                                                                ua_avg_query_time: double,
                                                                ua_query_type_percent: double
                                                            >
                                                        >      COMMENT 'List of top user-agents (by query count) using this subgraph and other aggregate info about its queries. The percents are w.r.t the subgraphs data',
                `ua_query_count_percentiles`           array<double>  COMMENT 'List of 0.1 to 0.9 percentile of query count per user-agent in each subgraph',
                `ua_query_count_mean`                  double  COMMENT 'Mean of query count per user-agent in each subgraph',
                `subgraph_composition`                 array<
                                                            struct<
                                                                item: boolean,
                                                                predicate: boolean,
                                                                uri: boolean,
                                                                qid: boolean,
                                                                literal: boolean,
                                                                count: bigint
                                                                >
                                                            >  COMMENT 'List of various combinations is which queries match with a subgraph and the number of such queries',
                `query_only_accessing_this_subgraph`   bigint  COMMENT 'Number of queries that access only this subgraph alone',
                `top_items`                            map<string, bigint>  COMMENT 'Top items in this subgraph that caused a query match mapped to the number of queries that matched',
                `matched_items_percentiles`            array<double>  COMMENT 'List of 0.1 to 0.9 percentile of queries matched per item in each subgraph',
                `matched_items_mean`                   double  COMMENT 'Mean of queries matched per item in this subgraph',
                `top_predicates`                       map<string, bigint>  COMMENT 'Top predicates in this subgraph that caused a query match mapped to the number of queries that matched',
                `matched_predicates_percentiles`       array<double>  COMMENT 'List of 0.1 to 0.9 percentile of queries matched per predicate in each subgraph',
                `matched_predicates_mean`              double  COMMENT 'Mean of queries matched per predicate in this subgraph',
                `top_uris`                             map<string, bigint>  COMMENT 'Top URIs in this subgraph that caused a query match mapped to the number of queries that matched',
                `matched_uris_percentiles`             array<double>  COMMENT 'List of 0.1 to 0.9 percentile of queries matched per URI in each subgraph',
                `matched_uris_mean`                    double  COMMENT 'Mean of queries matched per URI in this subgraph',
                `service_counts`                       map<string, bigint>  COMMENT 'List of services and the number of queries that use the service',
                `path_counts`                          map<string, bigint>  COMMENT 'List of top paths used and the number of queries that use the path'
            )
            PARTITIONED BY (
                `year`              int     COMMENT 'Unpadded year of queries',
                `month`             int     COMMENT 'Unpadded month of queries',
                `day`               int     COMMENT 'Unpadded day of queries',
                `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_per_subgraph_query_metrics_location }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.subgraph_pair_query_metrics_table }} (
                `subgraph1`         string  COMMENT 'First subgraph of the subgraph pair',
                `subgraph2`         string  COMMENT 'Second subgraph of the subgraph pair',
                `query_count`       bigint  COMMENT 'Number of queries that access subgraph1 and subgraph2'
            )
            PARTITIONED BY (
                `year`              int     COMMENT 'Unpadded year of queries',
                `month`             int     COMMENT 'Unpadded month of queries',
                `day`               int     COMMENT 'Unpadded day of queries',
                `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_subgraph_pair_query_metrics_location }}'
            ;
            """  # noqa
    ) >> complete

with DAG(
        'subgraph_metrics_weekly',
        default_args=default_args,
        # all input tables are available every monday
        # schedule this dag for monday to keep the dates in sync
        # wikidata dumps by 6 UTC and other tables take ~30 min more
        schedule_interval='0 7 * * 1',
        # As a weekly job there should never really be more than
        # one running at a time.
        max_active_runs=1,
        catchup=True,
) as subgraph_metrics_dag:
    wikidata_table_and_partition: str = '%s/date=%s/wiki=%s' % (WIKIDATA_TABLE, DATE, WIKI)
    all_subgraphs_table_and_partition: str = '%s/snapshot=%s/wiki=%s' % (
        ALL_SUBGRAPHS_TABLE, DATE, WIKI)
    top_subgraph_items_table_and_partition: str = '%s/snapshot=%s/wiki=%s' % (
        TOP_SUBGRAPH_ITEMS_TABLE, DATE, WIKI)
    top_subgraph_triples_table_and_partition: str = '%s/snapshot=%s/wiki=%s' % (
        TOP_SUBGRAPH_TRIPLES_TABLE, DATE, WIKI)
    general_subgraph_metrics_table: str = '%s/snapshot=%s/wiki=%s' % (
        GENERAL_SUBGRAPH_METRICS_TABLE, DATE, WIKI)

    wait_for_data = NamedHivePartitionSensor(
        task_id='wait_for_data',
        mode='reschedule',
        sla=timedelta(days=1),
        retries=4,
        partition_names=[wikidata_table_and_partition,
                         all_subgraphs_table_and_partition,
                         top_subgraph_items_table_and_partition,
                         top_subgraph_triples_table_and_partition],
    )

    extract_general_subgraph_metrics = SparkSubmitOperator(
        task_id='extract_general_subgraph_metrics',
        conf={
            # Delegate retries to airflow
            'spark.yarn.maxAppAttempts': '1',
        },
        application=WDQS_SPARK_TOOLS,
        pool='sequential',
        java_class="org.wikidata.query.rdf.spark.metrics.SubgraphMetricsLauncher",
        max_executors=64,
        executor_cores=4,
        executor_memory="8g",
        driver_memory="8g",
        application_args=[
            "general-subgraph-metrics",
            "--wikidata-triples-table", wikidata_table_and_partition,
            "--all-subgraphs-table", all_subgraphs_table_and_partition,
            "--top-subgraph-items-table", top_subgraph_items_table_and_partition,
            "--top-subgraph-triples-table", top_subgraph_triples_table_and_partition,
            "--general-subgraph-metrics-table", general_subgraph_metrics_table,
            "--min-items", MIN_ITEMS
        ]
    )

    extract_detailed_subgraph_metrics = SparkSubmitOperator(
        task_id='extract_detailed_subgraph_metrics',
        conf={
            # Delegate retries to airflow
            'spark.yarn.maxAppAttempts': '1',
        },
        application=WDQS_SPARK_TOOLS,
        pool='sequential',
        java_class="org.wikidata.query.rdf.spark.metrics.SubgraphMetricsLauncher",
        max_executors=64,
        executor_cores=4,
        executor_memory="8g",
        driver_memory="8g",
        application_args=[
            "detailed-subgraph-metrics",
            "--all-subgraphs-table", all_subgraphs_table_and_partition,
            "--top-subgraph-items-table", top_subgraph_items_table_and_partition,
            "--top-subgraph-triples-table", top_subgraph_triples_table_and_partition,
            "--general-subgraph-metrics-table", general_subgraph_metrics_table,
            "--min-items", MIN_ITEMS,
            "--per-subgraph-metrics-table",
            '%s/snapshot=%s/wiki=%s' % (PER_SUBGRAPH_METRICS_TABLE, DATE, WIKI),
            "--subgraph-pair-metrics-table",
            '%s/snapshot=%s/wiki=%s' % (SUBGRAPH_PAIR_METRICS_TABLE, DATE, WIKI),
        ]
    )
    complete = DummyOperator(task_id='complete')

    (wait_for_data
     >> extract_general_subgraph_metrics
     >> extract_detailed_subgraph_metrics  # depends on the output table from previous task
     >> complete)

with DAG(
        'subgraph_query_metrics_daily',
        default_args=default_args,
        schedule_interval='@daily',
        max_active_runs=4,
        catchup=True,
) as subgraph_query_metrics_dag:
    subgraph_query_table_and_partition: str = '%s/%s/wiki=%s' % (
        SUBGRAPH_QUERY_MAPPING_TABLE, YMD_PARTITION, WIKI)
    processed_external_sparql_query_table_and_partitions: str = '%s/%s/wiki=%s' % (
        PROCESSED_QUERY_TABLE, YMD_PARTITION, WIKI)
    subgraph_qitem_match_table_and_partition: str = '%s/%s/wiki=%s' % (
        SUBGRAPH_QITEM_MATCH_TABLE, YMD_PARTITION, WIKI)
    subgraph_predicate_match_table_and_partition: str = '%s/%s/wiki=%s' % (
        SUBGRAPH_PREDICATE_MATCH_TABLE, YMD_PARTITION, WIKI)
    subgraph_uri_match_table_and_partition: str = '%s/%s/wiki=%s' % (
        SUBGRAPH_URI_MATCH_TABLE, YMD_PARTITION, WIKI)

    wait_for_event_sparql_queries = NamedHivePartitionSensor(
        task_id='wait_for_event_sparql_queries',
        mode='reschedule',
        sla=timedelta(days=1),
        retries=4,
        partition_names=eventgate_partitions(EVENT_SPARQL_QUERY_TABLE, YMD_PARTITION)
    )

    wait_for_subgraph_query_table = NamedHivePartitionSensor(
        task_id='wait_for_subgraph_query_table',
        mode='reschedule',
        sla=timedelta(days=1),
        retries=4,
        partition_names=[subgraph_query_table_and_partition]
    )

    wait_for_data = NamedHivePartitionSensor(
        task_id='wait_for_data',
        mode='reschedule',
        sla=timedelta(days=1),
        retries=4,
        partition_names=[
            processed_external_sparql_query_table_and_partitions,
            subgraph_qitem_match_table_and_partition,
            subgraph_predicate_match_table_and_partition,
            subgraph_uri_match_table_and_partition
        ]
    )

    extract_subgraph_query_metrics = SparkSubmitOperator(
        task_id='extract_subgraph_query_metrics',
        conf={
            # Delegate retries to airflow
            'spark.yarn.maxAppAttempts': '1',
        },
        application=WDQS_SPARK_TOOLS,
        pool='sequential',
        java_class="org.wikidata.query.rdf.spark.metrics.SubgraphMetricsLauncher",
        max_executors=64,
        executor_cores=4,
        executor_memory="8g",
        driver_memory="8g",
        application_args=[
            "query-metrics",
            "--event-query-table",
            '%s/%s/wiki=%s' % (EVENT_SPARQL_QUERY_TABLE, YMD_PARTITION, WIKI),
            "--processed-query-table", processed_external_sparql_query_table_and_partitions,
            "--matched-Qitems-table", subgraph_qitem_match_table_and_partition,
            "--matched-predicates-table", subgraph_predicate_match_table_and_partition,
            "--matched-uris-table", subgraph_uri_match_table_and_partition,
            "--subgraph-query-table", subgraph_query_table_and_partition,
            "--top-n", TOP_N,
            "--general-query-metrics-table",
            '%s/%s/wiki=%s' % (GENERAL_QUERY_METRICS_TABLE, YMD_PARTITION, WIKI),
            "--general-subgraph-query-metrics-table",
            '%s/%s/wiki=%s' % (GENERAL_SUBGRAPH_QUERY_METRICS_TABLE, YMD_PARTITION, WIKI),
            "--per-subgraph-query-metrics-table",
            '%s/%s/wiki=%s' % (PER_SUBGRAPH_QUERY_METRICS_TABLE, YMD_PARTITION, WIKI),
        ]
    )

    extract_subgraph_pair_query_metrics = SparkSubmitOperator(
        task_id='extract_subgraph_pair_query_metrics',
        conf={
            # Delegate retries to airflow
            'spark.yarn.maxAppAttempts': '1',
        },
        application=WDQS_SPARK_TOOLS,
        pool='sequential',
        java_class="org.wikidata.query.rdf.spark.metrics.SubgraphMetricsLauncher",
        max_executors=64,
        executor_cores=4,
        executor_memory="8g",
        driver_memory="8g",
        application_args=[
            "subgraph-pair-query-metrics",
            "--subgraph-query-table", subgraph_query_table_and_partition,
            "--subgraph-pair-query-metrics-table",
            '%s/%s/wiki=%s' % (SUBGRAPH_PAIR_QUERY_METRICS_TABLE, YMD_PARTITION, WIKI),
        ]
    )

    complete = DummyOperator(task_id='complete')

    ([wait_for_event_sparql_queries, wait_for_subgraph_query_table, wait_for_data]
        >> extract_subgraph_query_metrics
        >> complete)

    wait_for_subgraph_query_table >> extract_subgraph_pair_query_metrics >> complete
