CREATE TABLE `mjolnir.query_clicks_ltr`(
  `query` string,
  `timestamp` bigint,
  `wikiid` string,
  `project` string,
  `session_id` string,
  `request_set_token` string,
  `hit_page_ids` array<int>,
  `click_page_ids` array<int>
)
PARTITIONED BY (
  `date` string
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/mjolnir/query_clicks_ltr'
;
