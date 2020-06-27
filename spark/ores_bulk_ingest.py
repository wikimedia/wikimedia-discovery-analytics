#!/usr/bin/python3
# -*- coding: utf-8 -*-
from argparse import ArgumentParser
from datetime import datetime
import itertools
import logging
from typing import Iterator, Mapping, Sequence, Tuple, TypeVar

from pyspark.sql import SparkSession, functions as F, types as T
import mwapi
import oresapi

T_ = TypeVar('T_')

MW_API_BATCH_SIZE = 500
ORES_API_BATCH_SIZE = 1000
MODEL = 'articletopic'


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
        if self.error_rate > self.threshold:
            raise Exception('Exceeded error threshold of {}, {}.'.format(
                self.threshold, self.status))


def fetch_scores(
    mediawiki_host: str, mediawiki_dbname: str,
    ores_host: str, user_agent: str,
    namespaces: Sequence[int],
    error_threshold=ErrorThreshold(1 / 1000),
) -> Iterator[Tuple[int, int, Mapping[str, float]]]:
    """Fetch scores for all main namespace pages of wiki."""
    logging.info('Querying mediawiki api at {}'.format(mediawiki_host))
    api = mwapi.Session(mediawiki_host, user_agent=user_agent)
    logging.info('Querying ores api at {}'.format(ores_host))
    ores = oresapi.Session(ores_host, user_agent=user_agent)

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
    pages = (
        page
        for batch in page_batches
        for page in batch['query']['pages'])
    page_w_latest_rev = ((page['pageid'], page['ns'], page['revisions'][0]['revid']) for page in pages)
    page_w_latest_rev_batches = make_batch(page_w_latest_rev, ORES_API_BATCH_SIZE)

    for batch in page_w_latest_rev_batches:
        page_ids, page_namespaces, rev_ids = zip(*batch)
        logging.info('Scoring revision batch')
        scores = ores.score(mediawiki_dbname, [MODEL], rev_ids)
        for page_id, page_namespace, data in zip(page_ids, page_namespaces, scores):
            try:
                yield (page_id, page_namespace, data[MODEL]['score']['probability'])
                error_threshold.incr(error=False)
            except KeyError:
                if 'error' not in data[MODEL]:
                    raise
                error_threshold.incr(error=True)

    logging.info('Finished scoring for {}. {}'.format(
        mediawiki_dbname, error_threshold.status))


def main():
    def date(val: str) -> datetime:
        return datetime.strptime(val, '%Y-%m-%d')

    parser = ArgumentParser()
    parser.add_argument('--mediawiki-dbname', required=True, help='dbname, ex: arwiki')
    parser.add_argument('--ores-host', default='https://ores.wikimedia.org')
    parser.add_argument('--user-agent', default='Search Platform Airflow Bot')
    parser.add_argument(
        '--output-table', required=True,
        help='Hive table to write exported predictions to')
    parser.add_argument(
        '--date', type=date, required=True,
        help='Date to use for output table partition specification')
    parser.add_argument(
        '--namespace', nargs='+', type=int, default=[0],
        help='Set of namespaces to export. By default only NS_MAIN is exported')

    args = parser.parse_args()

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
    mediawiki_host = lookup_hostname(spark, args.mediawiki_dbname)

    rdd = (
        spark.sparkContext.parallelize([True], numSlices=1)
        .flatMap(lambda x: fetch_scores(
            mediawiki_host, args.mediawiki_dbname, args.ores_host, args.user_agent, args.namespaces))
    )

    df = spark.createDataFrame(rdd, T.StructType([
        T.StructField('page_id', T.IntegerType()),
        T.StructField('page_namespace', T.IntegerType()),
        T.StructField('probability', T.MapType(T.StringType(), T.FloatType()))
    ]))

    df.createTempView('tmp_revision_score_out')

    insert_stmt = """
        INSERT OVERWRITE TABLE {table}
        PARTITION(wikiid="{wiki}", model="{model}", year={year}, month={month}, day={day})
        SELECT page_id, page_namespace, probability
        FROM tmp_revision_score_out
    """.format(
        table=args.output_table,
        year=args.date.year,
        month=args.date.month,
        day=args.date.day,
        wiki=args.mediawiki_dbname,
        model=MODEL)

    spark.sql(insert_stmt)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
