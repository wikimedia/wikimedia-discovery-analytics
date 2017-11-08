-- The hourly data for query_clicks is quite small, and we never need the
-- data at single hour resolution. Save some work reading all the data by
-- collapsing the 24 hourly partitions into a single daily partition.
--
-- Parameters:
--     cirrussearchrequestset_table
--                       -- Fully qualified table name of cirrus search request logs
--     source_table      -- Fully qualified table name to source data from
--     destination_table -- Fully qualified table name to write data to
--     session_timeout   -- Number of seconds with a search after which a search
--                          session restarts.
--     year              -- Year of partition to compute
--     month             -- Month of partition to compute
--     day               -- Day of partition to compute
--
-- Usage:
--     hive -f query_clicks_daily.hql
--          -d cirrussearchrequestset_table=wmf_raw.cirrussearchrequestset
--          -d source_table=discovery.query_clicks_hourly
--          -d destination_table=discovery.query_clicks_daily
--          -d session_timeout=1800
--          -d year=2016
--          -d month=12
--          -d day=4

-- Be explicit about the compression format to use
SET parquet.compression = SNAPPY;

-- Enable result file merging
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
-- Merge if files are < 128MB
set hive.merge.smallfiles.avgsize=134217728;
set hive.merge.size.per.task=134217728;
-- Target size 256MB per file
set mapred.max.split.size=268435456;
set mapred.min.split.size=268435456;

-- Measures the time between search requests, marking a new session whenever an
-- identity has spent more than ${session_timeout} seconds between searches.
-- See https://www.dataiku.com/learn/guide/code/reshaping_data/sessionization.html
-- for more details
WITH sessionized AS (
    SELECT
        *,
        -- The first session in each day per identity is null, convert those into
        -- session 0 and everything else into session 1+
        CONCAT_WS('_', identity, CAST(IF(session_num IS NULL, 0, session_num + 1) AS string)) AS session_id
    FROM (
        SELECT
            *,
            -- Sums the new session markers (each either 1 or 0) from the begining up to the current
            -- row, giving the number of new sessions that have occured prior to this row..
            SUM(CAST(new_session AS int)) OVER (PARTITION BY identity ORDER BY timestamp) AS session_num
        FROM (
            SELECT
                *,
                -- Records a 1 each time the time difference between current and previous search
                -- by this identity is larger than the session timeout.
                timestamp - LAG(timestamp) OVER (PARTITION BY identity ORDER BY timestamp) >= ${session_timeout} AS new_session
            FROM (
                SELECT
                    *,
                    COUNT(1) OVER (PARTITION BY identity) AS rows_by_identity
                FROM
                    ${source_table}
                WHERE
                    year = ${year} AND month=${month} AND day=${day}
            ) x
            WHERE
                -- We have some identities that make hundreds of thousands of queries a day.
                -- The later sum(new_session) over (...) expression chokes on those identities,
                -- essentially summing each possible set of rows. We don't really need that data
                -- for ML purposes, so filter it early.
                rows_by_identity < 1000
        ) y
    ) z
),

-- Count the number of queries issued by a single ip over the day.
-- This allows consumers of the data to apply brute force automata
-- removal by choosing some threshold they are happy with.
q_by_ip_day_tbl AS (
    SELECT
        ip,
        COUNT(1) as count
    FROM
        ${cirrussearchrequestset_table}
    WHERE
        year = ${year} AND month=${month} AND day=${day}
        AND source = 'web'
    GROUP BY
        ip
)

INSERT OVERWRITE TABLE
    ${destination_table}
PARTITION(year=${year},month=${month},day=${day})
SELECT
    sessionized.query,
    q_by_ip_day_tbl.count AS q_by_ip_day,
    sessionized.timestamp,
    sessionized.wikiid,
    sessionized.project,
    sessionized.hits,
    sessionized.clicks,
    sessionized.session_id
FROM
    sessionized
JOIN
    q_by_ip_day_tbl
ON
    sessionized.ip = q_by_ip_day_tbl.ip
WHERE
    -- Filter down to searches with clicks
    sessionized.clicks IS NOT NULL
;
