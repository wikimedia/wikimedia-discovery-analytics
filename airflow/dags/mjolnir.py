from datetime import datetime, timedelta
from typing import NewType

from airflow.operators.dummy_operator import DummyOperator

from wmf_airflow import DAG
from wmf_airflow.mjolnir import MjolnirOperator
from wmf_airflow.swift_upload import SwiftUploadOperator
from wmf_airflow.hive_table_path import HiveTablePath


# Set of wikis to train
from wmf_airflow.template import REPO_PATH

WIKIS = [
    'arwiki', 'dewiki', 'enwiki', 'fawiki',
    'fiwiki', 'frwiki', 'hewiki', 'idwiki',
    'itwiki', 'jawiki', 'kowiki', 'nlwiki',
    'nowiki', 'plwiki', 'ptwiki', 'ruwiki',
    'svwiki', 'viwiki', 'zhwiki',
]

# Feature set to collect. Must exist on prod search clusters.
FEATURE_SET = '20180215-query_explorer'

# Default tables. Some of these may not be real tables in hive, in partiticular
# training_files and trained_model simply write files where a partition would
# if it was in hive. Output file locations will be determined based on hive
# metastore data about these tables.
NAME_NODE = 'hdfs://analytics-hadoop'
BASE_DATA_DIR = NAME_NODE + '/wmf/data/discovery/mjolnir'
TABLES = {
    'query_clicks_raw': 'discovery.query_clicks_daily',
    'query_clicks': 'mjolnir.query_clicks_ltr',
    'query_clustering': 'mjolnir.query_clustering',
    'labeled_query_page': 'mjolnir.labeled_query_page',
    'feature_vectors': 'mjolnir.feature_vectors',
    'training_files': BASE_DATA_DIR + '/training_files',
    'model_parameters': 'mjolnir.model_parameters',
    'trained_models': BASE_DATA_DIR + '/trained_models',
}

# Paths to deployed resources for MjolnirOperator
deploys = {
    'mjolnir_venv': REPO_PATH + '/environments/mjolnir/venv.zip',
    'refinery': NAME_NODE + '/wmf/refinery/current',
    'discovery-analytics': REPO_PATH,
}

# Shared CLI args for scripts that talk with kafka
KAFKA_CLI_ARGS = {
    'brokers': ','.join([
        'kafka-jumbo1001.eqiad.wmnet:9092',
        'kafka-jumbo1002.eqiad.wmnet:9092',
        'kafka-jumbo1003.eqiad.wmnet:9092']),
    'topic-request': 'mjolnir.msearch-prod-request',
    'topic-response': 'mjolnir.msearch-prod-response',
}

# Types for each of the output formats. Perhaps there should be separate
# classes instead of a single MjolnirOperator. Having types for the operators
# makes it more explicit which tasks are valid inputs to other tasks.
# TODO: Make mypy find MjolnirOperator
QueryClicks = NewType('QueryClicks', MjolnirOperator)  # type: ignore
QueryClustering = NewType('QueryClustering', MjolnirOperator)  # type: ignore
LabeledQueryPage = NewType('LabeledQueryPage', MjolnirOperator)  # type: ignore
FeatureVectors = NewType('FeatureVectors', MjolnirOperator)  # type: ignore
TrainingFiles = NewType('TrainingFiles', MjolnirOperator)  # type: ignore
ModelParameters = NewType('ModelParameters', MjolnirOperator)  # type: ignore
TrainedModel = NewType('TrainedModel', MjolnirOperator)  # type: ignore


def query_clicks_ltr() -> QueryClicks:
    op = MjolnirOperator(
        task_id='query_clicks_ltr',
        table=TABLES['query_clicks'],
        partition_spec=[
            ('date', '{{ ds_nodash }}'),
        ],
        transformer_args={
            'input-table': TABLES['query_clicks_raw'],
            'output-table': TABLES['query_clicks'],
            'max-q-by-day': 50
        })
    return QueryClicks(op)


def norm_query(clicks: QueryClicks) -> QueryClustering:
    op = MjolnirOperator(
        task_id='norm_query_clustering',
        table=TABLES['query_clustering'],
        partition_spec=[
            ('date', '{{ ds_nodash }}'),
            ('algorithm', 'norm_query'),
        ],
        spark_args={'driver_memory': '8g'},
        transformer_args=dict(KAFKA_CLI_ARGS, **{
            'clicks-table': clicks._table,
            'output-table': TABLES['query_clustering'],
            'top-n': 5,
            'min-sessions-per-query': 10,
        }))
    clicks >> op
    return QueryClustering(op)


def dbn(clicks: QueryClicks, clusters: QueryClustering) -> LabeledQueryPage:
    # TODO: Output partitioning doesn't take clusters into account
    op = MjolnirOperator(
        task_id='dbn-' + clusters.partition_key('algorithm'),
        transformer='dbn',
        table=TABLES['labeled_query_page'],
        partition_spec=[
            ('date', '{{ ds_nodash }}'),
            ('algorithm', 'dbn')
        ],
        spark_args=dict(conf={
            'spark.executor.cores': 4,
            'spark.executor.memory': '6g',
            'spark.sql.shuffle.partitions': 5000,
        }),
        transformer_args={
            'clicks-table': clicks._table,
            'clustering-table': clusters._table,
            'clustering-algo': clusters.partition_key('algorithm'),
            'output-table': TABLES['labeled_query_page'],
        })
    clicks >> op
    clusters >> op
    return LabeledQueryPage(op)


def collect_vectors(
    clicks: QueryClicks,
    clusters: QueryClustering,
    feature_set: str
) -> FeatureVectors:
    # TODO: Output partitioning doesn't take inputs into account. We should
    # probably at least be able to vary the elasticsearch feature set name from
    # the feature set name written to disk.
    op = MjolnirOperator(
        task_id='feature_vectors-{}-{}'.format(
            clusters.partition_key('algorithm'), feature_set),
        transformer='feature_vectors',
        table=TABLES['feature_vectors'],
        partition_spec=[
            ('date', '{{ ds_nodash }}'),
            ('feature_set', feature_set)
        ],
        marker='_METADATA.JSON',
        transformer_args=dict(KAFKA_CLI_ARGS, **{
            'clicks-table': clicks._table,
            'clustering-table': clusters._table,
            'clustering-algorithm': clusters.partition_key('algorithm'),
            'output-table': TABLES['feature_vectors'],
            # Wiki's is accessed globaly, rather than parameterized,
            # as the output partitioning isn't per-wiki and needs
            # all of them.
            'wikis': WIKIS,
            'feature-set': feature_set,
            # Maximum number of records we can train against in a single
            # instance. Practically this only effects enwiki.
            'samples-per-wiki': 27000000,
        }))
    clicks >> op
    clusters >> op
    return FeatureVectors(op)


def prune_vectors(
    vectors: FeatureVectors,
    labels: LabeledQueryPage,
    feature_set: str
) -> FeatureVectors:
    op = MjolnirOperator(
        task_id='feature_selection-{}-{}-{}'.format(
            vectors.partition_key('feature_set'), labels.partition_key('algorithm'),
            feature_set),
        transformer='feature_selection',
        table=TABLES['feature_vectors'],
        partition_spec=[
            ('date', '{{ ds_nodash }}'),
            ('feature_set', feature_set),
        ],
        marker='_METADATA.JSON',
        spark_args=dict(
            driver_memory='6g',
            packages=','.join([
                'sramirez:spark-infotheoretic-feature-selection:1.4.4',
                'sramirez:spark-MDLP-discretization:1.4.1']),
            conf={
                'spark.executor.cores': 2,
                'spark.executor.memory': '5g',
                'spark.locality.wait': 0
            }),
        transformer_args={
            'feature-vectors-table': vectors._table,
            'feature-set': vectors.partition_key('feature_set'),
            'labels-table': labels._table,
            'labeling-algorithm': labels.partition_key('algorithm'),
            'output-table': TABLES['feature_vectors'],
            'wikis': WIKIS,

            'output-feature-set': feature_set,
            'num-features': 50,
        })
    labels >> op
    vectors >> op
    return FeatureVectors(op)


def make_folds(wiki: str, vectors: FeatureVectors, labels: LabeledQueryPage) -> TrainingFiles:
    op = MjolnirOperator(
        task_id='make_folds-{}-{}-{}'.format(
            wiki, labels.partition_key('algorithm'),
            vectors.partition_key('feature_set')),
        transformer='make_folds',
        table=TABLES['training_files'],
        partition_spec=[
            ('date', '{{ ds_nodash }}'),
            ('wikiid', wiki),
            ('labeling_algorithm', labels.partition_key('algorithm')),
            ('feature_set', vectors.partition_key('feature_set')),
        ],
        marker='_METADATA.JSON',
        auto_size_metadata_dir=vectors._output_path,
        transformer_args={
            'feature-vectors-table': vectors._table,
            'feature-set': vectors.partition_key('feature_set'),
            'labels-table': labels._table,
            'labeling-algorithm': labels.partition_key('algorithm'),
            'wiki': wiki,

            'num-folds': 5,
        })
    vectors >> op
    labels >> op
    return TrainingFiles(op)


def hyperparam(training_files: TrainingFiles) -> ModelParameters:
    op = MjolnirOperator(
        task_id='hyperparam-{wikiid}-{labeling_algorithm}-{feature_set}'.format(
            **dict(training_files._partition_spec)),
        pool='sequential',
        transformer='hyperparam',
        table=TABLES['model_parameters'],
        partition_spec=training_files._partition_spec,
        auto_size_metadata_dir=training_files._output_path,
        spark_args=dict(
            driver_memory='3g',
            conf={
                'spark.dynamicAllocation.executorIdleTimeout': '180s',
                'spark.task.cpus': 6,
                'spark.executor.cores': 6,
            }),
        transformer_args={
            'training-files-path': training_files._output_path,
            'output-table': TABLES['model_parameters'],

            # hyperparameter search configuration
            'initial-num-trees': 100,
            'final-num-trees': 500,
            'iterations': 150,
            'num-cv-jobs': 75,
        })
    training_files >> op
    return ModelParameters(op)


def train(
    training_files: TrainingFiles,
    model_parameters: ModelParameters,
    remote_feature_set: str
) -> TrainedModel:
    op = MjolnirOperator(
        task_id='train-{wikiid}-{labeling_algorithm}-{feature_set}'.format(
            **dict(training_files._partition_spec)),
        transformer='train',
        table=TABLES['trained_models'],
        partition_spec=training_files._partition_spec,
        marker='_METADATA.JSON',
        auto_size_metadata_dir=training_files._output_path,
        spark_args=dict(
            driver_memory='2g',
            conf={
                'spark.task.cpus': 6,
                'spark.executor.cores': 6,
            }),
        transformer_args={
            'model-parameters-table': model_parameters._table,
            'training-files-path': training_files._output_path,
            'remote-feature-set': remote_feature_set,
        })
    training_files >> op
    model_parameters >> op
    return TrainedModel(op)


def upload(trained_model: TrainedModel) -> SwiftUploadOperator:
    wiki = trained_model.partition_key('wikiid')
    op = SwiftUploadOperator(
        task_id='upload-{}-{}-{}'.format(
            wiki, trained_model.partition_key('labeling_algorithm'),
            trained_model.partition_key('feature_set')),
        swift_overwrite=True,
        swift_delete_after=timedelta(days=7),
        source_directory=trained_model._output_path,
        swift_container='search_mjolnir_model',
        swift_object_prefix='{{ ds_nodash }}',
        swift_auto_version=True)
    return trained_model >> op


with DAG(
    'mjolnir',
    default_args={
        'start_date': datetime(2020, 1, 8),
        # Defaults used by MjolnirOperator
        'deploys': deploys,
    },
    schedule_interval=timedelta(days=7),
    # If we don't run for a given week there is no use in re-running it,
    # the process always reads the full query_clicks history.
    catchup=False,
    user_defined_filters={
        'hive_table_path': HiveTablePath(),
    },
) as dag:
    clicks = query_clicks_ltr()
    clusters = norm_query(clicks)
    labels = dbn(clicks, clusters)
    raw_vectors = collect_vectors(clicks, clusters, FEATURE_SET)
    vectors = prune_vectors(raw_vectors, labels, '{}-pruned_mrmr'.format(FEATURE_SET))

    training_complete = DummyOperator(task_id='complete')
    for wiki in WIKIS:
        training_files = make_folds(wiki, vectors, labels)
        model_parameters = hyperparam(training_files)
        trained_model = train(training_files, model_parameters, FEATURE_SET)
        uploaded = upload(trained_model)
        # Join all wikis back to a single dag entry.
        uploaded >> training_complete
