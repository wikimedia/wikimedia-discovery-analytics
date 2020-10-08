from argparse import ArgumentParser
from collections import defaultdict
import json
import logging
import sys

try:
    from pyspark.sql import SparkSession
except ImportError:
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession

from pyspark.sql import DataFrame, functions as F, types as T

# Backport from spark 2.4.0
DataFrame.transform = lambda self, fn: fn(self)  # type: ignore


def arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('--cirrus-table', required=True)
    parser.add_argument('--satisfaction-table', required=True)
    parser.add_argument('--output-table', required=True)
    parser.add_argument('--year', required=True)
    parser.add_argument('--month', required=True)
    parser.add_argument('--day', required=True)
    return parser


def explode_and_flatten_list_of_structs(col_name):
    def transform(df):
        assert 'exploded' not in df.columns
        dataType = df.schema[col_name].dataType.elementType
        base_cols = list(set(df.columns) - {col_name})
        element_cols = ['exploded.' + name for name in dataType.fieldNames()]
        return (
            df
            .select(F.explode(col_name).alias('exploded'), *base_cols)
            .select(*(base_cols + element_cols))
        )
    return transform


class MinVal:
    value = None

    def aggregate(self, x):
        if self.value is None or x < self.value:
            self.value = x


def _parse_extra(extraParams):
    if extraParams is None:
        return {}
    try:
        decoded = json.loads(extraParams)
    except (TypeError, json.decoder.JSONDecodeError):
        # Log somehow? We Expect these to always be complete.
        return {}
    # json.loads returns None for the 'null' string. Not sure why but we've
    # seen this in real data...
    return {} if decoded is None else decoded


def dict_path(data, *path, default=None):
    """Retrieve a value from nested dictionaries by key path

    Similar to data.get(a, {}).get(b, {}).get(c, default) but with
    improved handling of error conditions when, for example, an
    element in the middle of the path is available but not a dict.
    """
    for piece in path:
        try:
            data = data[piece]
        except (KeyError, TypeError):
            return default
    return data


def as_dym_events(events):
    """Aggregate fulltext session into per-search satisfaction events

    Takes the full set of events that occured within a single fulltext search
    session and transforms them into an event per search with various boolean
    properties related to features we used/offered to answer the query and the
    users interaction with those results.

    This could plausibly be done directly in spark, but it seemed
    much more complex and required multiple shuffles to aggregate
    per-search and per-session data that is needed.
    """
    events = list(events)
    # Map from suggested query to first search token that suggested it
    suggested = defaultdict(MinVal)
    for event in events:
        if event['suggestion']:
            suggested[event['suggestion']].aggregate((event['dt'], event['searchToken']))
    suggested = {sugg: min_val.value[1] for sugg, min_val in suggested.items()}

    # set of searches that showed the result set of a dym query
    dym_searches = set()
    # set of searches that were autorewritten
    dym_autorewrite = set()
    # set of searches that showed a dym suggestion
    dym_shown = set()
    # set of searches that clicked the dym
    dym_clicked = set()
    # set of searches that had user interaction (click, etc)
    hit_interact = set()
    # dict from search token to number of results displayed
    hits_returned = dict()
    # dict from search token to minimum dt
    min_dt = defaultdict(MinVal)
    # map from search token to the main results provider
    results_from = {}
    # map from search token to the query suggestion provider
    suggestion_from = {}
    # set of searches where we should have found a source search for a suggestion but
    # could not (debug info).
    sugg_not_found = set()

    for event in events:
        is_serp = event['action'] == 'searchResultPage'
        is_autorewrite = is_serp and event['didYouMeanVisible'] == 'autorewrite'
        is_dym_visible = is_serp and event['didYouMeanVisible'] in ('autorewrite', 'yes')
        is_dym_clickthrough = is_serp and event['inputLocation'] in (
            'dym-suggest', 'dym-rewritten')
        is_dym = is_autorewrite or is_dym_clickthrough
        is_hit_interact = event['action'] in ('click', 'visitPage')

        min_dt[event['searchToken']].aggregate(event['dt'])

        if is_dym_visible:
            dym_shown.add(event['searchToken'])
        if is_dym:
            dym_searches.add(event['searchToken'])
        if is_autorewrite:
            dym_autorewrite.add(event['searchToken'])
        if is_dym_clickthrough:
            try:
                dym_clicked.add(suggested[event['query']])
            except KeyError:
                sugg_not_found.add(event['searchToken'])

        if is_serp:
            hits_returned[event['searchToken']] = 0 if event['hitsReturned'] is None else event['hitsReturned']
            # The source of query results and the suggestion (for example, which algorithm provided
            # the suggestion).
            extra = _parse_extra(event['extraParams'])
            results_from[event['searchToken']] = dict_path(
                extra, 'fallback', 'mainResults', 'name', default='n/a')
            suggestion_from[event['searchToken']] = dict_path(
                extra, 'fallback', 'querySuggestion', 'name', default='n/a')

        if is_hit_interact:
            hit_interact.add(event['searchToken'])

    dym_events = []
    for search in {event['searchToken'] for event in events}:
        dym_events.append((
            min_dt[search].value,
            search in dym_autorewrite,
            search in dym_searches,
            search in dym_shown,
            search in dym_clicked,
            hits_returned.get(search),
            search in hit_interact,
            results_from.get(search, 'n/a'),
            suggestion_from.get(search, 'n/a')
        ))
    return dym_events


# spark type for tuples emitted from as_dym_events
DYM_EVENT_TYPE = T.StructType([
    T.StructField('dt', T.StringType(), False),
    T.StructField('is_autorewrite_dym', T.BooleanType(), False),
    T.StructField('is_dym', T.BooleanType(), False),
    T.StructField('dym_shown', T.BooleanType(), False),
    T.StructField('dym_clicked', T.BooleanType(), False),
    T.StructField('hits_returned', T.IntegerType(), True),
    T.StructField('hit_interact', T.BooleanType(), False),
    T.StructField('results_provider', T.StringType(), False),
    T.StructField('sugg_provider', T.StringType(), False),
])


def transform_sessions(df):
    """Convert search satisfaction events into per-search dym events"""
    udf = F.udf(as_dym_events, T.ArrayType(DYM_EVENT_TYPE))

    # per-session metadata to collect
    geo_cols = [
        F.first(F.col('geocoded_data')[name]).alias(name)
        for name in ('continent', 'country_code', 'country', 'subdivision', 'city')]

    ua_cols = [
        F.first(F.col('useragent')[name]).alias('ua_' + name)
        for name in df.schema['useragent'].dataType.fieldNames()]

    agg_cols = [
        # a single session has a constant test bucket
        F.first(F.col('event.subTest')).alias('bucket'),
        # Sample multiplier should be constant too
        F.first(F.col('event.sampleMultiplier')).alias('sample_multiplier'),
        # source data for as_dym_events udf
        F.collect_list(F.struct(
            'dt', 'suggestion', 'event.action', 'event.didYouMeanVisible',
            'event.extraParams', 'event.hitsReturned', 'event.inputLocation',
            'event.query', 'event.searchSessionId', 'event.searchToken',
            'event.uniqueId')).alias('events')
    ]

    return (
        df
        .groupBy('wiki', 'event.searchSessionId')
        .agg(*agg_cols, *geo_cols, *ua_cols)
        .withColumn('dym_events', udf(F.col('events')))
        .drop('events')
        .transform(explode_and_flatten_list_of_structs('dym_events'))
    )


def extract_cirrus_suggestions(df):
    """Extract all provided suggestions from cirrus request logging.

    When a user clicks a DYM suggestion the resulting SERP event records
    that this is a suggested query, but not which search suggested it.
    Pull in the suggested queries so we can at least guess.
    """
    return (
        df.select(
            F.col('search_id').alias('searchToken'),
            F.explode(F.col('elasticsearch_requests.suggestion')).alias('suggestion'))
        .where(F.col('suggestion').isNotNull())
        .groupBy('searchToken')
        .agg(F.first('suggestion').alias('suggestion'))
    )


def select_expr(df, table_expr, partition_spec):
    out = df.sql_ctx.sparkSession.read.table(table_expr)
    for col_name in partition_spec.keys():
        out = out.drop(col_name)
    # spark columns are case insensitive
    expect_cols = [name.lower() for name in out.columns]
    found_cols = [name.lower() for name in df.columns]
    if set(expect_cols) != set(found_cols):
        raise Exception('Unexpected columns: {} != {}'.format(
            sorted(expect_cols), sorted(found_cols)))
    return ','.join(out.columns)


def write_partition(df, table_expr, partition_spec):
    if '.' not in table_expr:
        raise Exception("both database and table must be provided")
    output_db, output_table = table_expr.split('.', 1)
    temp_table = output_table + '_write_partition'
    df.createOrReplaceTempView(temp_table)

    sql = """
        INSERT OVERWRITE TABLE {table} PARTITION({partition_expr})
        SELECT {select_expr} FROM {temp_table}
    """.format(
        table=table_expr,
        select_expr=select_expr(df, table_expr, partition_spec),
        partition_expr=','.join(
            '{}="{}"'.format(k, v) for k, v in partition_spec.items()),
        temp_table=temp_table)

    spark = df.sql_ctx.sparkSession
    spark.sql(sql).collect()
    spark.catalog.dropTempView(temp_table)


def main(
    cirrus_table: str,
    satisfaction_table: str,
    output_table: str,
    year: str,
    month: str,
    day: str
) -> int:
    spark = SparkSession.builder.getOrCreate()

    partition_cond = (
        (F.col('year') == year)
        & (F.col('month') == month)
        & (F.col('day') == day))

    # We need this to lookup which searchToken gave a query suggestion and
    # give it credit for the clickthrough.
    # TODO: Produce an event against the old searchToken directly using the
    # referer instead of having to find it by joining cirrus logs.
    df_cirrus_sugg = extract_cirrus_suggestions(
        spark.read.table(cirrus_table).where(partition_cond))

    # TODO: What about sessions that cross daily boundaries? We would need
    # to determine which sessions are unfinished and put those in a second
    # table to be brought in to the next day of processing. For now we simply
    # report stats across daily boundaries incorrectly.

    df_events = (
        spark.read.table(satisfaction_table)
        .where(partition_cond)
        # autocomplete doesn't make sense on a per-search basis. Needs
        # separate session + page based events.
        .where(F.col('event.source') == 'fulltext')
        # these events aren't currently used
        .where(F.col('event.action') != 'checkin')
        .join(df_cirrus_sugg, how='left',
              on=F.col('searchToken') == F.col('event.searchToken'))
        # Drop the duplicate searchToken field, we still have event.searchToken
        .drop('searchToken')
    )

    df = transform_sessions(df_events)

    # Our daily dataset is tiny due to sampling at the source,
    # no need for multiple partitions.
    write_partition(df.repartition(1), output_table, {
        'year': year,
        'month': month,
        'day': day,
    })
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = arg_parser().parse_args()
    sys.exit(main(**dict(vars(args))))
