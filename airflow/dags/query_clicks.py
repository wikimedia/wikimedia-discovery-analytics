"""Transform backend search logs into query-click logs"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor

import jinja2
from wmf_airflow import DAG
from wmf_airflow.hive_partition_range_sensor import HivePartitionRangeSensor
from wmf_airflow.template import DagConf, wmf_conf, eventgate_partitions


dag_conf = DagConf('query_clicks_conf')

with DAG(
    'query_clicks_init',
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
    max_active_runs=1,
    catchup=False,
    user_defined_macros={
        'dag_conf': dag_conf.macro,
        'wmf_conf': wmf_conf.macro,
    },
    template_undefined=jinja2.StrictUndefined,
) as dag_init:
    HiveOperator(
        task_id='create_tables',
        hql=dedent("""
            CREATE TABLE IF NOT EXISTS {{ dag_conf.table_daily }}(
              `query` string,
              `q_by_ip_day` int,
              `timestamp` bigint,
              `wikiid` string,
              `project` string,
              `hits` array<struct<`title`:string,`index`:string,`pageid`:int,`score`:float,`profilename`:string>>,
              `clicks` array<struct<`pageid`:int,`timestamp`:bigint,`referer`:string>>,
              `session_id` string,
              `request_set_token` string
              -- new fields must be added to end to match alter table behaviour
            )
            PARTITIONED BY (
              `year` int,
              `month` int,
              `day` int
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_path_daily }}'
            ;

            CREATE TABLE IF NOT EXISTS {{ dag_conf.table_hourly }} (
              `query` string,
              `ip` string,
              `identity` string,
              `timestamp` bigint,
              `wikiid` string,
              `project` string,
              `hits` array<struct<
                  `title`:string,
                  `index`:string,
                  `pageid`:int,
                  `score`:float,
                  `profilename`:string
              >>,
              `clicks` array<struct<
                  `pageid`:int,
                  `timestamp`:bigint,
                  `referer`:string
              >>,
              `request_set_token` string
            )
            PARTITIONED BY (
              `year` int,
              `month` int,
              `day` int,
              `hour` int
            )
            STORED AS PARQUET
            LOCATION '{{ wmf_conf.data_path }}/{{ dag_conf.rel_path_hourly }}'
            ;
        """)  # noqa
    ) >> DummyOperator(task_id='complete')


with DAG(
    'query_clicks_hourly',
    schedule_interval='@hourly',
    start_date=datetime(2022, 2, 8),
    max_active_runs=1,
    catchup=True,
    user_defined_macros={
        'dag_conf': dag_conf.macro,
        # Generate row_timestamp to allow requesting the specified year / month
        # / day / hour, and the following hour, from similarly partitioned tables. As
        # long as all functions used are deterministic the hive query planner
        # will appropriately choose only 2 hourly partitions from the table.
        'row_timestamp': """
            TO_UNIX_TIMESTAMP(CONCAT(
                CONCAT_WS('-', CAST(year AS string), CAST(month AS string), CAST(day AS string)),
                ' ',
                CONCAT_WS(':', CAST(hour AS string), '00', '00')
            ))
        """
    },
    template_undefined=jinja2.StrictUndefined,
) as dag_hourly:
    sensors = [
        HivePartitionRangeSensor(
            task_id='wait_for_web_requests',
            table=dag_conf('table_webrequest'),
            # Each hourly run requires it's own hour plus the following hour
            period=timedelta(hours=2),
            partition_frequency='hours',
            partition_specs=[
                [
                    ('webrequest_source', 'text'),
                    ('year', None), ('month', None),
                    ('day', None), ('hour', None),
                ]
            ]
        ),
        NamedHivePartitionSensor(
            task_id='wait_for_cirrus_requests',
            timeout=60 * 60 * 6,  # 6 hours
            partition_names=eventgate_partitions(dag_conf('table_cirrus_requests')),
        ),
    ]

    # Joins full text search request logs against web request logs to generate
    # a table that represents the full text search results presented to individual
    # users along with which results they clicked. This data is useful both for
    # generating metrics about how the search engine is performing, as well as
    hive_hourly_op = HiveOperator(
        task_id='transform_hourly',
        hql=dedent("""
            -- Be explicit about the compression format to use
            SET parquet.compression = SNAPPY;

            -- Sometimes mappers get killed for using slightly more than the 1G default. Lets try 1.5G
            SET mapreduce.map.memory.mb=2000;

            -- TODO: Test release version, why are we on a snapshot? We should use a release so
            -- it can be stored in artifacts/ and pulled from archiva like normal jars.
            ADD JAR hdfs://analytics-hadoop/user/ebernhardson/refinery-hive-0.0.91-SNAPSHOT.jar;

            CREATE TEMPORARY FUNCTION get_pageview_info AS 'org.wikimedia.analytics.refinery.hive.GetPageviewInfoUDF';
            CREATE TEMPORARY FUNCTION get_main_search_request AS 'org.wikimedia.analytics.refinery.hive.GetMainSearchRequestUDF';

            -- Collect web requests to pages that have a searchToken in
            -- the referer. These should all be clicks from Special:Search
            -- to an article. Groups the clicks by the project + search token
            -- to provide a list of clicks generated from a single search.
            WITH web_req AS (
                SELECT
                    pageview_info['project'] AS project,
                    STR_TO_MAP(PARSE_URL(referer, 'QUERY'), '&', '=')['searchToken'] AS search_token,
                    -- This isn't actually the complete set of clicks against a query. When a user clicks
                    -- forward, doesnt like the result, then clicks back, they get a new search token. We
                    -- actually need to re-group the data by (query,session_id) to get that info...The problem
                    -- is search_req.hits might not be exactly the same, so that will have to be handled
                    -- individually by consumers of this data.
                    COLLECT_LIST(NAMED_STRUCT(
                        -- pageid in webreq changed from int to bigint nov 2020, cast back
                        -- to int and delay updating downstream as the largest page_id today
                        -- is ~100M.
                        'pageid', CAST(page_id as int),
                        -- For some reason hive refuses to read a table with a timestamp field in
                        -- the array<struct<...>>. Normalize to unix timestamps instead.
                        'timestamp', TO_UNIX_TIMESTAMP(ts),
                        -- mostly for debugging purposes
                        'referer', referer
                    )) AS clicks
                FROM
                    {{ dag_conf.table_webrequest }}
                WHERE
                    -- The second hour of webrequest is needed so searches that happen towards the end
                    -- of an hour and have clicks in the begining of the next hour are still associated.
                    {{ row_timestamp }} >= {{ execution_date.int_timestamp }}
                    AND {{ row_timestamp }} <= {{ execution_date.add(hours=1, minutes=59).int_timestamp }}
                    AND webrequest_source = 'text'
                    -- Users clicking navigational elements will match the search token,
                    -- but it won't be a pageview with page_id
                    AND is_pageview = TRUE
                    AND page_id IS NOT NULL
                    -- The request must have our search token in the referer. STR_TO_MAP
                    -- also requires non-null input.
                    AND PARSE_URL(referer, 'QUERY') IS NOT NULL
                    AND STR_TO_MAP(PARSE_URL(referer, 'QUERY'), '&', '=')['searchToken'] IS NOT NULL
                GROUP BY
                    pageview_info['project'],
                    STR_TO_MAP(PARSE_URL(referer, 'QUERY'), '&', '=')['searchToken']
            ),

            -- Generate a map from the dbname we store in cirrussearchrequestset to the
            -- project name stored in the webrequests table.
            namespace_map AS (
                SELECT DISTINCT
                    dbname,
                    get_pageview_info(hostname, '', '')['project'] AS project
                FROM
                    {{ dag_conf.table_namespace_map }}
                WHERE
                    snapshot = '{{ macros.hive.max_partition(dag_conf.table_namespace_map).decode('ascii') }}'
            ),

            -- Collect full text search requests against Special:Search.
            search_req AS (
                SELECT
                    csrs.elasticsearch_requests[SIZE(csrs.elasticsearch_requests)-1].query AS query,
                    -- This would return a bigint, but we know we shouldnt ever seen anything
                    -- that even fills an int.
                    csrs.http.client_ip,
                    namespace_map.project,
                    csrs.`database`,
                    csrs.identity,
                    csrs.search_id AS request_set_token,
                    csrs.meta.dt,
                    get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).hits AS hits
                FROM
                    {{ dag_conf.table_cirrus_requests }} csrs
                JOIN
                    namespace_map
                ON
                    namespace_map.dbname = csrs.`database`
                WHERE
                    year = {{ execution_date.year }}
                    AND month = {{ execution_date.month }}
                    AND day = {{ execution_date.day }}
                    AND hour = {{ execution_date.hour }}
                    -- We only want requests from the web, because their clicks are recorded
                    -- in webrequests
                    AND csrs.source = 'web'
                    -- Only take requests that include a full text search against the current wiki
                    -- (excludes completion suggester and other outliers).
                    AND get_main_search_request(csrs.`database`, csrs.elasticsearch_requests) IS NOT NULL
                    -- Make sure we only extract from content index
                    AND SIZE(get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).indices) == 1
                    AND (
                        get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).indices[0] RLIKE '.*_(content|file)'
                        OR
                        (
                            -- Comonswiki has different defaults which results in the standard query hitting
                            -- the top level alias.
                            csrs.`database` == 'commonswiki'
                            AND get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).indices[0] == 'commonswiki'
                            -- Since we dont have _content to filter non-content queries, restrict to the default
                            -- selected namespaces. Also hive doesnt have an array_equals function...
                            AND size(get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).namespaces) == 6
                            AND array_contains(get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).namespaces, CAST(0 AS BIGINT))
                            AND array_contains(get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).namespaces, CAST(6 AS BIGINT))
                            AND array_contains(get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).namespaces, CAST(12 AS BIGINT))
                            AND array_contains(get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).namespaces, CAST(14 AS BIGINT))
                            AND array_contains(get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).namespaces, CAST(100 AS BIGINT))
                            AND array_contains(get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).namespaces, CAST(106 AS BIGINT))
                        )
                    )
                    -- Only fetch first page for simplicity
                    AND get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).hits_offset = 0
                    -- We only want 'normal' requests here. if the user requested more than
                    -- the default 20 results filter them out
                    AND SIZE(get_main_search_request(csrs.`database`, csrs.elasticsearch_requests).hits) <= 21
            ),

            -- When converting from the CirrusSearchRequests avro schema to mediawiki.cirrussearch-request json schema
            -- the names and types of the hits field changed. To avoid changing all the downstream tasks reshape hits
            -- from the json format into the old avro format.
            search_req_old AS (
                SELECT
                    request_set_token,
                    query,
                    client_ip as ip,
                    identity,
                    unix_timestamp(dt, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS `timestamp`,
                    wikiid,
                    project,
                    -- Sorts ascending based on first struct property
                    sort_array(collect_list(named_struct(
                        'hit_pos', hit_pos,
                        'hit', named_struct(
                            'title', hit.page_title,
                            'index', hit.index,
                            'pageid', CAST(hit.page_id AS int),
                            'score', CAST(hit.score AS float),
                            'profilename', hit.profile_name
                        )
                    ))).hit as hits
                FROM (
                    SELECT
                        query, client_ip, identity, dt, `database` as wikiid, project, request_set_token,
                        hit_pos, hit
                    FROM
                        search_req
                    LATERAL VIEW
                        POSEXPLODE(hits) hit_ AS hit_pos, hit
                ) x
                GROUP BY
                    query, client_ip, identity, dt, wikiid, project, request_set_token
            )

            INSERT OVERWRITE TABLE
                {{ dag_conf.table_hourly }}
            PARTITION(
                year={{ execution_date.year }},
                month={{ execution_date.month }},
                day={{ execution_date.day }},
                hour={{ execution_date.hour }}
            )
            -- Join our search request data against clicks in web_request.
            SELECT
                -- Order here must match the create_table statement
                search_req_old.query,
                search_req_old.ip,
                search_req_old.identity,
                search_req_old.`timestamp`,
                search_req_old.wikiid,
                search_req_old.project,
                search_req_old.hits,
                web_req.clicks,
                search_req_old.request_set_token
            FROM
                search_req_old
            -- left join ensures we keep all full text search requests, even if they didn't
            -- have any recorded clicks. This is necessary for the script that merges hourly
            -- data into daily to be able to sessionize correctly.
            LEFT JOIN
                web_req
            ON
                web_req.search_token = search_req_old.request_set_token
                AND web_req.project = search_req_old.project
            ;
        """)  # noqa
    )

    sensors >> hive_hourly_op >> DummyOperator(task_id='complete')


with DAG(
    'query_clicks_daily',
    schedule_interval='@daily',
    start_date=datetime(2022, 2, 8),
    max_active_runs=1,
    catchup=True,
    user_defined_macros={
        'dag_conf': dag_conf.macro,
    },
    template_undefined=jinja2.StrictUndefined,
) as dag_daily:
    sensor = HivePartitionRangeSensor(
        task_id='wait_for_hourlies',
        table=dag_conf('table_hourly'),
        period=timedelta(days=1),
        partition_frequency='hours',
        partition_specs=[
            [
                ('year', None), ('month', None),
                ('day', None), ('hour', None),
            ]
        ])

    # Sessionize hourly partitions into single daily partition.  The daily
    # table additionally *DROPS* search sessions that do not include click
    # throughs. When no-click sessions are necessary the more verbose and
    # un-sessionized hourly tables must be referenced
    hive_daily_op = HiveOperator(
        task_id='transform_daily',
        hql=dedent("""
            -- Be explicit about the compression format to use
            SET parquet.compression = SNAPPY;

            -- Measures the time between search requests, marking a new session whenever an
            -- identity has spent more than {{ dag_conf.session_timeout_sec }} seconds between searches.
            -- See https://www.dataiku.com/learn/guide/code/reshaping_data/sessionization.html
            -- for more details
            WITH sessionized AS (
                SELECT
                    *,
                    -- The first session in each day per identity is null, convert those into
                    -- session 0 and everything else into session 1+
                    -- Include ymd in session id so they are unique across days.
                    CONCAT_WS('_', CAST(year AS string), CAST(month AS string), CAST(day AS string), identity, CAST(IF(session_num IS NULL, 0, session_num + 1) AS string)) AS session_id
                FROM (
                    SELECT
                        *,
                        -- Sums the new session markers (each either 1 or 0) from the begining up to the current
                        -- row, giving the number of new sessions that have occured prior to this row..
                        SUM(CAST(new_session AS int)) OVER (PARTITION BY identity ORDER BY `timestamp`) AS session_num
                    FROM (
                        SELECT
                            *,
                            -- Records a 1 each time the time difference between current and previous search
                            -- by this identity is larger than the session timeout.
                            `timestamp` - LAG(`timestamp`) OVER (PARTITION BY identity ORDER BY `timestamp`) >= {{ dag_conf.session_timeout_sec }} AS new_session
                        FROM (
                            SELECT
                                *,
                                COUNT(1) OVER (PARTITION BY identity) AS rows_by_identity,
                                COUNT(1) OVER (PARTITION BY ip) AS q_by_ip_day
                            FROM
                                {{ dag_conf.table_hourly }}
                            WHERE
                                year = {{ execution_date.year }}
                                AND month = {{ execution_date.month }}
                                AND day = {{ execution_date.day }}
                        ) x
                        WHERE
                            -- We have some identities that make hundreds of thousands of queries a day.
                            -- The later sum(new_session) over (...) expression chokes on those identities,
                            -- essentially summing each possible set of rows. We dont really need that data
                            -- for ML purposes, so filter it early.
                            rows_by_identity < 1000
                    ) y
                ) z
            )

            INSERT OVERWRITE TABLE
                {{ dag_conf.table_daily }}
            PARTITION(
                year = {{ execution_date.year }},
                month = {{ execution_date.month }},
                day = {{ execution_date.day }})
            SELECT
                -- Order here must match the create_table statement
                sessionized.query,
                sessionized.q_by_ip_day,
                sessionized.`timestamp`,
                sessionized.wikiid,
                sessionized.project,
                sessionized.hits,
                sessionized.clicks,
                sessionized.session_id,
                sessionized.request_set_token
            FROM
                sessionized
            WHERE
                -- Filter down to searches with clicks
                sessionized.clicks IS NOT NULL
            ;
        """)  # noqa
    )

    sensor >> hive_daily_op >> DummyOperator(task_id='complete')
