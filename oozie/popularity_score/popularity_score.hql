-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          aggregation for
--     destination_table -- Fully qualified table name to fill in aggregated
--                          values
--     year              -- inclusive year to end aggregation on
--     month             -- inclusive month to end aggregation on
--     day               -- inclusive day to end aggregation on
--     days_aggregated   -- number of days to aggregate over
--
-- Usage:
--     hive -f popularity_score.hql
--         -d source_table=wmf.pageview_hourly
--         -d destination_table=discovery.popularity_score
--         -d year=2016
--         -d month=10
--         -d day=3
--         -d days_aggregated=7
--

SET partquet.compression = SNAPPY;
SET mapred.reduce.tasks  = 6;

SET hivevar:end_date = TO_DATE(CONCAT_WS('-', CAST(${year} AS string), CAST(${month} AS string), CAST(${day} AS string)));
SET hivevar:start_date = DATE_SUB(${end_date}, ${days_aggregated});
SET hivevar:row_date = TO_DATE(CONCAT_WS('-', CAST(year AS string), CAST(month AS string), CAST(day AS string)));

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(agg_days=${days_aggregated},year=${year},month=${month},day=${day})
    SELECT
        hourly.project,
        hourly.page_id,
        SUM(hourly.view_count) / agg.view_count AS score
    FROM
        ${source_table} hourly
    JOIN (
        SELECT
            project,
            SUM(view_count) AS view_count
        FROM
            ${source_table}
        WHERE
            page_id IS NOT NULL
            AND ${row_date} BETWEEN ${start_date} AND ${end_date}
        GROUP BY
            project
       ) agg ON hourly.project = agg.project
    WHERE
        hourly.page_id IS NOT NULL
        AND ${row_date} BETWEEN ${start_date} AND ${end_date}
    GROUP BY
        hourly.project,
        hourly.page_id,
        agg.view_count
;
