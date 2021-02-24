#!/usr/bin/python3
# -*- coding: utf-8 -*-
from argparse import ArgumentParser
from datetime import datetime
import itertools
import logging
import sys
from typing import Iterator, Mapping, Optional, Sequence, Tuple, TypeVar

from pyspark.sql import SparkSession, functions as F, types as T
import mwapi
import oresapi
from wmf_spark import HivePartitionWriter

T_ = TypeVar('T_')

MW_API_BATCH_SIZE = 500
ORES_API_BATCH_SIZE = 1000
MODEL = 'articletopic'


def arg_parser() -> ArgumentParser:
    def date(val: str) -> datetime:
        return datetime.strptime(val, '%Y-%m-%d')

    parser = ArgumentParser()
    parser.add_argument(
        '--mediawiki-dbname', required=True,
        help='dbname, ex: arwiki')
    parser.add_argument('--ores-host', default='https://ores.wikimedia.org')
    parser.add_argument('--user-agent', default='Search Platform Airflow Bot')
    parser.add_argument(
        '--output-partition', required=True, type=HivePartitionWriter.from_spec,
        help='Hive partition to write exported predictions to')
    parser.add_argument(
        '--namespace', dest='namespaces', nargs='+', type=int, default=[0],
        help='Set of namespaces to export. By default only NS_MAIN is exported')
    parser.add_argument(
        '--ores-model', default='articletopic',
        help='ORES model to export scores for')
    return parser


def make_batch(iterable: Iterator[T_], size: int) -> Iterator[Sequence[T_]]:
    iterable = iter(iterable)
    next_batch = tuple(itertools.islice(iterable, size))
    while next_batch:
        yield next_batch
        next_batch = tuple(itertools.islice(iterable, size))


def lookup_hostname(spark: SparkSession, dbname: str) -> str:
    rows = (
        spark.read.table('canonical_data.wikis')
        .where(F.col('database_code') == dbname)
        .select('domain_name')
        .collect()
    )
    if len(rows) == 0:
        raise Exception('No wiki found matching dbname: {}'.format(dbname))
    elif len(rows) > 1:
        raise Exception('Multiple wikis found matching dbname?!? {}'.format(dbname))
    else:
        return 'https://' + rows[0].domain_name


class ErrorThreshold:
    _min_seen = ORES_API_BATCH_SIZE

    def __init__(self, threshold: float):
        self.threshold = threshold
        self._seen = 0
        self._errors = 0

    @property
    def error_rate(self) -> float:
        return 0. if self._seen == 0 else self._errors / self._seen

    @property
    def status(self) -> str:
        return 'Seen {} items with {} errors'.format(self._seen, self._errors)

    def incr(self, error=False) -> None:
        self._seen += 1
        if error is False:
            return
        self._errors += 1
        if self._seen >= self._min_seen and self.error_rate > self.threshold:
            raise Exception('Exceeded error threshold of {}, {}.'.format(
                self.threshold, self.status))


def all_pages(api: mwapi.Session, namespaces: Sequence[int]) -> Iterator[Mapping]:
    # While this API is called 'allpages', it's actually all pages in a single
    # namespace. Since we need more than one namespace, run once for each.
    all_ns_page_batches = [api.get(
        formatversion=2,
        action='query',
        generator='allpages',
        gapnamespace=namespace,
        # Cirrus only indexes full pages, redirects are a property
        # of the page they link to. Ergo, we don't need them
        gapfilterredir='nonredirects',
        gaplimit=MW_API_BATCH_SIZE,
        prop='revisions',
        rvprop='ids',
        continuation=True,
    ) for namespace in set(namespaces)]
    page_batches = itertools.chain(*all_ns_page_batches)
    for batch in page_batches:
        # wmf enables miser mode which means some batchs may be empty.
        for page in batch.get('query', {}).get('pages', []):
            yield page


def score_one_batch(
    ores: oresapi.Session,
    mediawiki_dbname: str,
    rev_ids: Sequence[int],
    error_threshold: ErrorThreshold,
    retries=5
) -> Sequence[Optional[Mapping[str, float]]]:
    while retries:
        scores = list(ores.score(mediawiki_dbname, [MODEL], rev_ids))
        # When the request fails oresapi returns a single error
        if len(scores) == 1 and 'error' in scores[0][MODEL]:
            retries -= 1
            continue
        probs = []
        for score in scores:
            probability = score.get(MODEL, {}).get('score', {}).get('probability', None)
            error_threshold.incr(error=probability is None)
            probs.append(probability)
        assert len(rev_ids) == len(probs)
        return probs
    # Split batch into smaller pieces and figure out which ids don't work?
    raise RuntimeError('Ran out of retries fetching batch scores from ores')


def fetch_scores(
    mediawiki_host: str,
    mediawiki_dbname: str,
    ores_host: str,
    model: str,
    user_agent: str,
    namespaces: Sequence[int],
    error_threshold=ErrorThreshold(1 / 1000),
) -> Iterator[Tuple[int, int, Mapping[str, float]]]:
    """Fetch scores for all main namespace pages of wiki."""
    # spark doesn't init logging on executors
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)
    logging.info('Querying mediawiki api at {}'.format(mediawiki_host))
    api = mwapi.Session(mediawiki_host, user_agent=user_agent)
    logging.info('Querying ores api at {}'.format(ores_host))
    ores = oresapi.Session(ores_host, user_agent=user_agent)

    pages = all_pages(api, namespaces)
    page_w_latest_rev = ((page['pageid'], page['ns'], page['revisions'][0]['revid']) for page in pages)

    page_w_latest_rev_batches = make_batch(page_w_latest_rev, ORES_API_BATCH_SIZE)

    for batch in page_w_latest_rev_batches:
        page_ids, page_namespaces, rev_ids = zip(*batch)
        logging.info('Scoring revision batch')
        scores = score_one_batch(ores, mediawiki_dbname, rev_ids, error_threshold)
        for page_id, page_namespace, probability in zip(page_ids, page_namespaces, scores):
            if probability is not None:
                yield (page_id, page_namespace, probability)

    logging.info('Finished scoring for {}. {}'.format(
        mediawiki_dbname, error_threshold.status))


def main(
    mediawiki_dbname: str,
    ores_host: str,
    ores_model: str,
    user_agent: str,
    output_partition: HivePartitionWriter,
    namespaces: Sequence[int]
):
    # Basically a hack, here we create an rdd with a single row and use it to
    # run our function on the executor.  Once we have an rdd representing the
    # exported scores convert it to a DataFrame and write to a hive table.
    # Spark integration gets us hdfs output, chunked output (multiple
    # partitions on read), and hive integration for "free".
    #
    # The single output partition is not a problem, in testing spark is able to
    # read back a single 3.5G partition emitted this way as many 128MB
    # partitions. Additionally spark has no particular issues with very long
    # running tasks (as long as no shuffling is involved), such as exporting
    # enwiki (>24 hours to export), beyond the intrinsic problem of needing to
    # restart from the beginning if it fails.

    spark = SparkSession.builder.getOrCreate()
    mediawiki_host = lookup_hostname(spark, mediawiki_dbname)

    rdd = (
        spark.sparkContext.parallelize([True], numSlices=1)
        .flatMap(lambda x: fetch_scores(
            mediawiki_host, mediawiki_dbname,
            ores_host, ores_model,
            user_agent, namespaces))
    )

    df = spark.createDataFrame(rdd, T.StructType([  # type: ignore
        T.StructField('page_id', T.IntegerType()),
        T.StructField('page_namespace', T.IntegerType()),
        T.StructField('probability', T.MapType(T.StringType(), T.FloatType()))
    ]))

    output_partition.overwrite_with(df)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
