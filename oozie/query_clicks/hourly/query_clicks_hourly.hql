-- Joins full text search request logs against web request logs to generate
-- a table that represents the full text search results presented to individual
-- users along with which results they clicked. This data is useful both for
-- generating metrics about how the search engine is performing, as well as
--
-- Paremeters:
--     refinery_jar_version
--                       -- Version of the refinery jar to import for UDFs
--     analytics_artifacts_directory
--                       -- The artifact directory where to find
--                          jar files to import for UDFs
--     source_cirrus_table
--                       -- Fully qualified table name to source CirrusSearch
--                          requests from
--     source_webrequest_table
--                       -- Fully qualified table name to source webrequest
--                          data from
--     source_namespace_map_table
--                       -- Fully qualified table name to source the map from
--                          webrequest projects to cirrus dbname
--     source_namespace_map_snapshot_id
--                       -- Id of the snapshot (partition) of the source_namespace_map_table
--                          to query.
--     destination_table -- Fully qualified table name to store the
--                          computed click data in.
--     yaer              -- year of partition to compute
--     month             -- month of partition to compute
--     day               -- day of partition to compute
--     hour              -- hour of partition to compute
--
--
--
-- Usage:
--     hive -f query_clicks.hql                                                   \
--          -d refinery_jar_version=0.40                                          \
--          -d artifacts_directory=/wmf/refinery/current/artifacts                \
--          -d source_cirrus_table=wmf_raw.cirrussearchrequestset                 \
--          -d source_webrequest_table=wmf.webrequest                             \
--          -d source_namespace_map_table=wmf_raw.mediawiki_project_namespace_map \
--          -d source_namespace_map_snapshot_id=2018-10                           \
--          -d destination_table=discovery.query_clicks_hourly                    \
--          -d year=2016                                                          \
--          -d month=12                                                           \
--          -d day=4                                                              \
--          -d hour=16
--


-- Be explicit about the compression format to use
SET parquet.compression = SNAPPY;

-- Sometimes mappers get killed for using slightly more than the 1G default. Lets try 1.5G
SET mapreduce.map.memory.mb=1500;

--ADD JAR ${analytics_artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}.jar;
ADD JAR hdfs://analytics-hadoop/user/ebernhardson/refinery-hive-0.0.39-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION get_pageview_info AS 'org.wikimedia.analytics.refinery.hive.GetPageviewInfoUDF';
CREATE TEMPORARY FUNCTION get_main_search_request AS 'org.wikimedia.analytics.refinery.hive.GetMainSearchRequestUDF';

-- Generate row_timestamp, start_timestamp and end_timestamp variables to allow
-- requesting the specified year/month/day/hour, and the following hour, from the
-- webrequest table. As long as all functions used are deterministic the hive
-- query planner will appropriately choose only 2 hourly partitions from the webrequest
-- table.
-- The second hour of webrequest is needed so searches that happen towards the end
-- of an hour and have clicks in the begining of the next hour are still associated.
SET hivevar:start_date = CONCAT_WS('-', CAST(${year} AS string), CAST(${month} AS string), CAST(${day} AS string));
SET hivevar:start_time = CONCAT_WS(':', CAST(${hour} AS string), '00', '00');
SET hivevar:start_timestamp = TO_UNIX_TIMESTAMP(CONCAT(${start_date}, ' ', ${start_time}));
SET hivevar:end_timestamp = (${start_timestamp} + 7199);

SET hivevar:row_date = CONCAT_WS('-', CAST(year AS string), CAST(month AS string), CAST(day AS string));
SET hivevar:row_time = CONCAT_WS(':', CAST(hour AS string), '00', '00');
SET hivevar:row_timestamp = TO_UNIX_TIMESTAMP(CONCAT(${row_date}, ' ', ${row_time}));

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
            'pageid', page_id,
            -- For some reason hive refuses to read a table with a timestamp field in
            -- the array<struct<...>>. Normalize to unix timestamps instead.
            'timestamp', TO_UNIX_TIMESTAMP(ts),
            -- mostly for debugging purposes
            'referer', referer
        )) AS clicks
    FROM
        ${source_webrequest_table}
    WHERE
        -- See comments above when creating *_timestamp vairables
        ${row_timestamp} >= ${start_timestamp}
        AND ${row_timestamp} <= ${end_timestamp}
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
        ${source_namespace_map_table}
    WHERE
        snapshot = "${source_namespace_map_snapshot_id}"
),

-- Collect full text search requests against Special:Search
search_req AS (
    SELECT
        csrs.requests[SIZE(csrs.requests)-1].query AS query,
        -- This would return a bigint, but we know we shouldn't ever seen anything
        -- that even fills an int.
        csrs.ip,
        namespace_map.project AS project,
        csrs.wikiid,
        csrs.identity,
        csrs.id AS request_set_token,
        csrs.ts AS timestamp,
        get_main_search_request(csrs.wikiid, csrs.requests).hits AS hits
    FROM
        ${source_cirrus_table} csrs
    JOIN
        namespace_map
    ON
        namespace_map.dbname = csrs.wikiid
    WHERE
        year = ${year} AND month = ${month} AND day = ${day} AND hour = ${hour}
        -- We only want requests from the web, because their clicks are recorded
        -- in webrequests
        AND csrs.source = 'web'
        -- Only take requests that include a full text search against the current wiki
        -- (excludes completion suggester and other outliers).
        AND get_main_search_request(csrs.wikiid, csrs.requests) IS NOT NULL
        -- Make sure we only extract from content index
        AND SIZE(get_main_search_request(csrs.wikiid, csrs.requests).indices) == 1
        AND (
            get_main_search_request(csrs.wikiid, csrs.requests).indices[0] RLIKE '.*_(content|file)'
            OR
            (
                -- Comonswiki has different defaults which results in the standard query hitting
                -- the top level alias.
                csrs.wikiid == 'commonswiki'
                AND get_main_search_request(csrs.wikiid, csrs.requests).indices[0] == 'commonswiki'
                -- Since we don't have _content to filter non-content queries, restrict to the default
                -- selected namespaces. Also hive doesn't have an array_equals function...
                AND size(get_main_search_request(csrs.wikiid, csrs.requests).namespaces) == 6
                AND array_contains(get_main_search_request(csrs.wikiid, csrs.requests).namespaces, 0)
                AND array_contains(get_main_search_request(csrs.wikiid, csrs.requests).namespaces, 6)
                AND array_contains(get_main_search_request(csrs.wikiid, csrs.requests).namespaces, 12)
                AND array_contains(get_main_search_request(csrs.wikiid, csrs.requests).namespaces, 14)
                AND array_contains(get_main_search_request(csrs.wikiid, csrs.requests).namespaces, 100)
                AND array_contains(get_main_search_request(csrs.wikiid, csrs.requests).namespaces, 106)
            )
        )
        -- Only fetch first page for simplicity
        AND get_main_search_request(csrs.wikiid, csrs.requests).hitsoffset = 0
        -- We only want 'normal' requests here. if the user requested more than
        -- the default 20 results filter them out
        AND SIZE(get_main_search_request(csrs.wikiid, csrs.requests).hits) <= 21
)

INSERT OVERWRITE TABLE
    ${destination_table}
PARTITION(year=${year},month=${month},day=${day},hour=${hour})
-- Join our search request data against clicks in web_request.
SELECT
    -- Order here must match the create_table statement
    search_req.query,
    search_req.ip,
    search_req.identity,
    search_req.timestamp,
    search_req.wikiid,
    search_req.project,
    search_req.hits,
    web_req.clicks,
    search_req.request_set_token
FROM
    search_req
-- left join ensures we keep all full text search requests, even if they didn't
-- have any recorded clicks. This is necessary for the script that merges hourly
-- data into daily to be able to sessionize correctly.
LEFT JOIN
    web_req
ON
    web_req.search_token = search_req.request_set_token
    AND web_req.project = search_req.project
;

