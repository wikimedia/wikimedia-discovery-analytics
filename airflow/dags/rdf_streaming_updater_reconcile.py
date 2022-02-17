"""
Process to collect discrepancies identified by the rdf-streaming-updater producer (flink app)
and schedule reconciliation events that are fed back to this same flink application.
This process assumes that an independent flink pipeline runs per DC and thus might
have encountered different discrepancies.
Since reconciliation events are shipped back via event-gate we do not entirely control
in which DC they might produced to. This means that reconciliation events for the flink
pipeline running in eqiad might be collected in a topic prefixed with "codfw".
In order for the flink pipeline to filter the reconciliation events that are related
to the discrepancies it found a source_tag is used (via --reconciliation-source in this spark
job).
This is also the reason unlike other jobs consuming MEP topics why this spark job is scheduled
on a per DC basis.
"""
from collections import namedtuple
from datetime import datetime, timedelta

import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor

from wmf_airflow import DAG
from wmf_airflow.spark_submit import SparkSubmitOperator
from wmf_airflow.template import REPO_PATH, DagConf, YMDH_PARTITION

ARTIFACTS_DIR = REPO_PATH + "/artifacts"
WDQS_SPARK_TOOLS = ARTIFACTS_DIR + '/rdf-spark-tools-latest-jar-with-dependencies.jar'

FLINK_APP_DCS = ['eqiad', 'codfw']
dag_conf = DagConf('rdf_streaming_updater_reconcile_conf')

lapsed_actions_table = dag_conf('lapsed_actions_table')
fetch_failures_table = dag_conf('fetch_failures_table')
state_inconsistencies_table = dag_conf('state_inconsistencies_table')

wdqs_source_tag_prefix = dag_conf('wdqs_source_tag_prefix')
wcqs_source_tag_prefix = dag_conf('wcqs_source_tag_prefix')

wdqs_events_domain = dag_conf('wdqs_events_domain')
wcqs_events_domain = dag_conf('wcqs_events_domain')

wdqs_entity_namespaces = dag_conf('wdqs_entity_namespaces')
wcqs_entity_namespaces = dag_conf('wcqs_entity_namespaces')

wdqs_initial_to_namespace_map = dag_conf('wdqs_initial_to_namespace_map')
wcqs_initial_to_namespace_map = dag_conf('wcqs_initial_to_namespace_map')

eventgate_endpoint = dag_conf('eventgate_endpoint')
http_routes_conf = dag_conf('http_routes')

reconciliation_stream = dag_conf('reconciliation_stream')


SideOutputs = namedtuple('SideOutputs',
                         ['lapsed_actions', 'fetch_failures', 'state_inconsistencies'])

SENSORS_SLA = timedelta(hours=6)


def wait_for_side_output(name: str, partition: str) -> NamedHivePartitionSensor:
    return NamedHivePartitionSensor(
        task_id='wait_for_' + name,
        #  re-enable once the backfill is comlete: sla=SENSORS_SLA,
        partition_names=[partition])


def wait_for_side_output_data(parts: SideOutputs):
    return [
        wait_for_side_output(name="lapsed_actions", partition=parts.lapsed_actions),
        wait_for_side_output(name="fetch_failures", partition=parts.fetch_failures),
        wait_for_side_output(name="state_inconsistencies", partition=parts.state_inconsistencies),
    ]


def submit_reconciliation(name: str,
                          domain: str,
                          source_tag: str,
                          entity_namespaces: str,
                          initials_to_namespace: str,
                          http_routes: str,
                          sideoutputs: SideOutputs) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=name,
        application=WDQS_SPARK_TOOLS,
        java_class="org.wikidata.query.rdf.updater.reconcile.UpdaterReconcile",
        max_executors=25,
        executor_cores=8,
        executor_memory="16g",
        driver_memory="4g",
        application_args=[
            "--domain", domain,
            "--reconciliation-source", source_tag,
            "--event-gate", eventgate_endpoint,
            "--late-events-partition", sideoutputs.lapsed_actions,
            "--failures-partition", sideoutputs.fetch_failures,
            "--inconsistencies-partition", sideoutputs.state_inconsistencies,
            "--stream", reconciliation_stream,
            "--entity-namespaces", entity_namespaces,
            "--initial-to-namespace", initials_to_namespace,
            "--http-routes", http_routes
        ],
    )


def get_sideoutputs_partitions(dc: str) -> SideOutputs:
    return SideOutputs(
        lapsed_actions="{}/datacenter={}/{}".format(lapsed_actions_table, dc, YMDH_PARTITION),
        fetch_failures="{}/datacenter={}/{}".format(fetch_failures_table, dc, YMDH_PARTITION),
        state_inconsistencies="{}/datacenter={}/{}".format(state_inconsistencies_table,
                                                           dc,
                                                           YMDH_PARTITION))


def build_dag(dag_id: str,
              start_date: datetime,
              source_tag: str,
              domain: str,
              entity_namespaces: str,
              initial_to_namespace_map: str) -> airflow.DAG:
    with DAG(
            dag_id=dag_id,
            default_args={
                'start_date': start_date,
                # This job is not technically speaking dependent on past, but we increase our
                # chances of making the reconciliations happier by processing them in order
                'depends_on_past': True,
            },
            schedule_interval='@hourly',
            # We want hourly runs to be scheduled one ofter the other
            max_active_runs=1,
            catchup=True
    ) as streaming_updater_reconcile:
        complete = DummyOperator(task_id='complete')
        for dc in FLINK_APP_DCS:
            # work on a per DC basis because the side-output topics are populated by the flink app
            # running in this DC, e.g. eqiad.rdf-streaming-updater.fetch-failures is populated by
            # flink@eqiad # We could think about adding a field to discriminate a particular flink
            # app but here it's identified using the datacenter (topic prefix) and meta.domain.
            sideoutputs_parts = get_sideoutputs_partitions(dc)
            wait_for_data = wait_for_side_output_data(sideoutputs_parts)
            job = submit_reconciliation(
                name='reconcile_{}'.format(dc),
                domain=domain,
                source_tag="{}@{}".format(source_tag, dc),
                entity_namespaces=entity_namespaces,
                initials_to_namespace=initial_to_namespace_map,
                http_routes=http_routes_conf,
                sideoutputs=sideoutputs_parts
            )
            wait_for_data >> job >> complete

    return streaming_updater_reconcile


wdqs_reconcile_dag = build_dag(dag_id='wdqs_streaming_updater_reconcile_hourly',
                               start_date=datetime(2022, 2, 18, 12, 00, 00),
                               source_tag=wdqs_source_tag_prefix,
                               domain=wdqs_events_domain,
                               entity_namespaces=wdqs_entity_namespaces,
                               initial_to_namespace_map=wdqs_initial_to_namespace_map)

wcqs_reconcile_dag = build_dag(dag_id='wcqs_streaming_updater_reconcile_hourly',
                               start_date=datetime(2022, 1, 13, 16, 00, 00),
                               source_tag=wcqs_source_tag_prefix,
                               domain=wcqs_events_domain,
                               entity_namespaces=wcqs_entity_namespaces,
                               initial_to_namespace_map=wcqs_initial_to_namespace_map)
