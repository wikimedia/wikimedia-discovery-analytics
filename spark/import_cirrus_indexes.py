from argparse import ArgumentParser
from collections import namedtuple
from functools import partial
import json
import logging
from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame, types as T
import random
import requests
from requests.adapters import HTTPAdapter
import sys
from typing import Dict, Iterator, List, Mapping, Optional, Sequence
from urllib3.util.retry import Retry  # type: ignore
from wmf_spark import HivePartitionWriter


# it seems dataclass interacts badly with cloudpickle in spark 2.4.4,
# use a named tuple to give some shape to this.
CirrusCluster = namedtuple('CirrusCluster', ('url', 'name', 'replica', 'group'))
CirrusIndex = namedtuple('CirrusIndex', ('cluster', 'url', 'num_shards', 'num_docs', 'store_bytes'))
# min_id is exclusive, max_id is inclusive. Both are strings(!!)
DataRequest = namedtuple('DataRequest', ('url', 'shard_id', 'batch_size', 'extra_cols', 'min_id', 'max_id'))


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('--elasticsearch', dest='source_clusters', required=True,
            type=lambda x: [val.strip() for val in x.split(',')],
            help='Comma delimited list of urls to elasticsearch clusters')
    parser.add_argument('--output-partition', required=True, type=HivePartitionWriter.from_spec)
    parser.add_argument('--retries', default=10, type=int)
    parser.add_argument('--debug', default=False, action='store_true')
    return parser


def make_https_session(retries: int = 5) -> requests.Session:
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(
        total=retries,
        backoff_factor=2,
        raise_on_status=True,
        status_forcelist=[429, 500, 502, 503, 504]
    )))
    return session


def to_bytes_by_suffix(val: str) -> int:
    for suffix, scale in {
        'tb': 2**40,
        'gb': 2**30,
        'mb': 2**20,
        'kb': 2**10,
        # must be last, or val.endswith('b') will match other suffixes
        'b': 1,
    }.items():
        if val.endswith(suffix):
            return int(float(val[:-len(suffix)]) * scale)
    raise Exception(f'Unparsable: {val}')


def parse_banner(url: str, banner: Mapping) -> CirrusCluster:
    # Identifies the cluster used based on the cluster banner
    #
    # Unfortunately the names we used for clusters aren't entirely consistent,
    # we need to use some domain knowledge to figure out the related cirrus
    # cluster/group, but it isn't completely standardized between clusters.
    #
    # example names:
    #  production-search-eqiad
    #  production-search-psi-codfw
    #  cloudelastic-chi-eqiad
    name_pieces = banner['cluster_name'].split('-')
    if name_pieces[0] == 'production':
        replica = name_pieces[-1]
        # the chi cluster has a historic name that doesn't include chi
        group = 'chi' if len(name_pieces) == 3 else name_pieces[2]
    elif name_pieces[0] == 'cloudelastic':
        replica = name_pieces[0]
        group = name_pieces[1]
    else:
        raise Exception(f'Unrecognized cluster name: {banner["cluster_name"]}')
    return CirrusCluster(
        url=url,
        name=banner['cluster_name'],
        replica=replica,
        group=group
    )


def parse_cat_aliases(cat_aliases: Sequence[Mapping]) -> Mapping[str, str]:
    # Limit to cirrus specific page indexes
    accept_suffixes = ('_content', '_general', '_file')
    aliases: Dict[str, str] = {}
    for index in cat_aliases:
        if not any(index['alias'].endswith(suffix) for suffix in accept_suffixes):
            continue
        if index['alias'] in aliases:
            raise Exception(f"Didn't expect to see index twice in aliases: {index['alias']}")
        aliases[index['index']] = index['alias']
    return aliases


def parse_cat_indices(
    cluster: CirrusCluster,
    cat_indices: Sequence[Mapping],
    aliases: Mapping[str, str],
) -> Iterator[CirrusIndex]:
    for index in cat_indices:
        if index['index'] not in aliases:
            continue
        yield CirrusIndex(
            cluster=cluster,
            url=f"{cluster.url}/{aliases[index['index']]}",
            num_shards=int(index['pri']),
            num_docs=int(index['docs.count']),
            store_bytes=to_bytes_by_suffix(index['pri.store.size'])
        )


def all_cirrus_indexes(base_url: str) -> Iterator[CirrusIndex]:
    with make_https_session() as session:
        res_banner = session.get(base_url)
        res_aliases = session.get(base_url + '/_cat/aliases', params={
            'format': 'json',
        })
        res_indices = session.get(base_url + '/_cat/indices', params={
            'format': 'json',
        })
    cluster = parse_banner(base_url, res_banner.json())
    aliases = parse_cat_aliases(res_aliases.json())
    return parse_cat_indices(cluster, res_indices.json(), aliases)


def index_to_data_requests(
    index: CirrusIndex,
    index_bytes_per_output_partition: int = to_bytes_by_suffix('256mb'),
    index_bytes_per_batch: int = to_bytes_by_suffix('10mb')
) -> Iterator[DataRequest]:
    # Split Shard into DataRequest's of approximately partition_bytes size
    #
    # This gives a useful property of having partitions that collect in
    # small(minutes) time frame instead of hours for a 50GB partition. It
    # reduces the per-partition memory necessary allowing for smaller executors
    # that are less likely to OOM (fewer outliers per partition). It also
    # auto-scales the batch size based on the size of docs, giving smaller
    # batches on indices with larger docs.
    #
    # Note that the store size and the size of documents we will ingest are not
    # equivalent, but correlated enough for our purposes. Additionally the _id
    # field is not equally distributed (as strings or ints) and the resulting
    # DataRequest's will still be quite lumpy in total collected size, but
    # hopefully reasonable enough to avoid OOM's while collecting large shards.
    if index.num_docs == 0:
        # brand new wikis can have 0 docs in one or both indices. similarly a
        # closed wiki can have 0 docs in their index.
        return
    est_doc_size = index.store_bytes / index.num_docs
    # Could cap batch size at hard limit of 10k, but 2k seems like enough?
    batch_size = min(2000, int(index_bytes_per_batch // est_doc_size))

    # Additional columns for output used for partitioning output data and
    # tracking provenance
    extra_cols = {
        "cirrus_group": index.cluster.group,
        "cirrus_replica": index.cluster.replica
    }

    def req_for_each_shard(start: Optional[str], end: Optional[str]) -> Iterator[DataRequest]:
        for shard_id in range(index.num_shards):
            yield DataRequest(index.url, shard_id, batch_size, extra_cols, start, end)

    partitions_per_shard = index.store_bytes / index.num_shards / index_bytes_per_output_partition
    if partitions_per_shard < 1:
        yield from req_for_each_shard(None, None)
        return

    # for sorting against strings we need to ensure everything is the same length.
    all_id_prefixes = [str(i) for i in range(100, 1000)]
    assert all(len(x) == 3 for x in all_id_prefixes)

    def id_prefix(i):
        return None if i is None else all_id_prefixes[i]

    # take every nth value from the list as the next partition ending value.
    # All skipped values become part of the partition. Sadly actually indexed
    # doc id's aren't evenly distributed with our prefixes so this is still
    # lumpy, but better than nothing. To improve we need to index page_id as an
    # integer value (still lumpy, but less so).
    # Start range at step, otherwise we would have (None, 100) as the first
    # request which should be an empty partition, no id starts with 0.
    step = int(len(all_id_prefixes) / partitions_per_shard)
    prev = None
    for i in range(step, len(all_id_prefixes), step):
        yield from req_for_each_shard(id_prefix(prev), id_prefix(i))
        prev = i
    yield from req_for_each_shard(id_prefix(prev), None)


def search_after(
    session: requests.Session,
    req: DataRequest,
    query: Dict
) -> Iterator[Mapping]:
    search_url = f'{req.url}/_search'
    params = {'preference': f'_shards:{req.shard_id}'}
    # assumes a single ascending sort
    # min_id is exclusive, max_id must be inclusive
    if req.min_id:
        query['search_after'] = [req.min_id]
    while True:
        data = session.get(search_url, json=query, params=params).json()
        if not data['hits']['hits']:
            # no more results available
            return
        for hit in data['hits']['hits']:
            # boldly assume elasticsearch and python sort strings the same way. Alternatively
            # if we indexed page_id as an int we could use a range query to get better
            # (and cheaper) guarantees.
            if req.max_id and hit['sort'][0] > req.max_id:
                return
            yield hit
        query['search_after'] = data['hits']['hits'][-1]['sort']


def collect_request(request: DataRequest, retries: int) -> Iterator[Mapping]:
    query = {
        "size": request.batch_size,
        "track_total_hits": False,
        "query": {"match_all": {}},
        "sort": [{'_id': 'asc'}]
    }
    with make_https_session(retries) as session:
        for hit in search_after(session, request, query):
            hit['_source']['page_id'] = int(hit['_id'])
            for k, v in request.extra_cols.items():
                if k in hit['_source']:
                    raise Exception(
                        f'DataRequest.extra_cols attempting to overwrite source column: {k}')
                hit['_source'][k] = v
            yield hit['_source']


def coerce_types(hit):
    if 'coordinates' in hit:
        # Spark SQL doesn't like that some floats are ints
        for coordinate in hit['coordinates']:
            coordinate['coord'] = {
                'lat': float(coordinate['coord']['lat']),
                'lon': float(coordinate['coord']['lon']),
            }

    if 'popularity_score' in hit:
        # Some page has a popularity of 1. Probably a decomissioned wiki with only Main_Page.
        hit['popularity_score'] = float(hit['popularity_score'])

    if 'file_text' in hit and (hit['file_text'] == [] or hit['file_text'] is False):
        # file_text is not particularly standardized. Should probably be fixed in cirrus
        hit['file_text'] = None

    if 'labels' in hit and hit['labels'] == []:
        # php json encodes the empty map as a list...should probably fix in cirrus?
        hit['labels'] = None

    if 'descriptions' in hit and hit['descriptions'] is not None:
        if hit['descriptions'] == []:
            # php json encodes the empty map as a list...should probably fix in cirrus?
            hit['descriptions'] = None
        else:
            for k, v in list(hit['descriptions'].items()):
                # It seems on wikidata descriptions are map<str, str>, but on commons
                # we see map<str, array<str>>. Make them all like commons.
                if isinstance(v, str):
                    hit['descriptions'][k] = [v]

    if 'defaultsort' in hit and hit['defaultsort'] is False:
        # Should be corrected in cirrus?
        del hit['defaultsort']

    return hit


def extra_fields_to_json(row, dest: str, known_fields: Sequence[str]):
    if dest in row:
        raise Exception('Destination field already exists')
    fields = set(known_fields)
    output = {}
    extra = {}
    for k, v in row.items():
        if k in fields:
            output[k] = v
        else:
            extra[k] = v
    output[dest] = json.dumps(extra)
    return output


def validate_type(value, data_type: T.DataType):
    if isinstance(data_type, T.ArrayType):
        if isinstance(value, list):
            for i, child in enumerate(value):
                for error in validate_type(child, data_type.elementType):
                    yield f'{i} : {error}'
        else:
            yield f'Expected list, got {type(value)}'
    elif isinstance(data_type, T.StringType):
        if not isinstance(value, str) and value is not None:
            yield f'Expected str, got {type(value)}'
    elif isinstance(data_type, (T.IntegerType, T.LongType)):
        if not isinstance(value, int) and value is not None:
            yield f'Expected int, got {type(value)}'
    elif isinstance(data_type, (T.FloatType, T.DoubleType)):
        if not isinstance(value, float) and value is not None:
            yield f'Expected float, got {type(value)}'
    elif isinstance(data_type, T.BooleanType):
        if not isinstance(value, bool):
            yield f'Expected bool, got {type(value)}'
    elif isinstance(data_type, T.MapType):
        if isinstance(value, dict):
            for k, v in value.items():
                yield from validate_type(k, data_type.keyType)
                yield from validate_type(v, data_type.valueType)
        else:
            yield f'Expected dict, got {type(value)}'
    elif isinstance(data_type, T.StructType):
        if isinstance(value, dict):
            for field in data_type:
                if field.name in value:
                    for error in validate_type(value[field.name], field.dataType):
                        yield f'{field.name} : {error}'
        else:
            yield f'Expected dict, got {type(value)}'


def validate_schema(row: Mapping, schema: T.StructType):
    # The errors emit by spark or avro when data is incorrect aren't all that
    # useful. Add an extra layer that can be debug invoked to tell us what
    # fields in which page are giving trouble.
    errors = list(validate_type(row, schema))
    if errors:
        # Print errors into executor output, then let it keep running
        print(f"Errors found validating {row['page_id']} at {row['wiki']}\n" + ', '.join(errors))


def partition_per_row(spark: SparkSession, rows: Sequence) -> RDD:
    return (
        # Enumerate to give a unique partition number per row
        spark.sparkContext.parallelize(list(enumerate(rows)))  # type: ignore
        # Apply that partition number
        .partitionBy(len(rows), lambda x: x)
        # Drop the partition number to have only the row data
        .map(lambda pair: pair[1])
    )


def collect_all_requests(
    spark: SparkSession,
    requests: Sequence[DataRequest],
    schema: T.StructType,
    retries: int,
    debug: bool
) -> DataFrame:
    fields = [field.name for field in schema.fields if field.name != 'extra_source']

    rdd = (
        # Use a partition per row since each row represents ~100MB of data to read
        partition_per_row(spark, requests)
        # Collect the data from each request
        .flatMap(partial(collect_request, retries=retries))
        # Move unknown source fields into a json blob
        .map(partial(extra_fields_to_json, dest='extra_source', known_fields=fields))
        # the document schema isn't particularly strict, normalize all the
        # types to something avro is happy to store.
        .map(coerce_types)
    )
    if debug:
        # Will print problems into executor logs. This uses foreach rather
        # than added into the normal pipeline so that it keeps running past
        # failures, instead of stopping when we get bad data.
        rdd.foreach(partial(validate_schema, schema=schema))
        raise Exception('Debug completed')
    return spark.createDataFrame(rdd, schema)


def guess_schema(spark: SparkSession, partition: HivePartitionWriter) -> T.StructType:
    return (
        partition.partition.read(spark)
        .drop(*partition.partition.partition_spec.keys())
        .schema
    )


def main(
    source_clusters: Sequence[str],
    output_partition: HivePartitionWriter,
    retries: int,
    debug: bool,
) -> int:
    spark = SparkSession.builder.getOrCreate()

    requests: List[DataRequest] = []
    for url in source_clusters:
        for index in all_cirrus_indexes(url):
            requests.extend(index_to_data_requests(index))

    # Issuing multiple data requests for a single shard means multiple executors can
    # be querying that single shard. Spark will execute these roughly in order, so
    # try and spread that out by shuffling.
    random.shuffle(requests)

    result_schema = guess_schema(spark, output_partition)
    df = collect_all_requests(spark, requests, result_schema, retries, debug)
    output_partition.overwrite_with(df)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
