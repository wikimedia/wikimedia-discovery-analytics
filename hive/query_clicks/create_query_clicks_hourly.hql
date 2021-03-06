CREATE TABLE `discovery`.`query_clicks_hourly` (
  `query` string,
  `ip` string,
  `identity` string,
  `timestamp` bigint,
  `wikiid` string,
  `project` string,
  `hits` array<struct<`title`:string,`index`:string,`pageid`:int,`score`:float,`profilename`:string>>,
  `clicks` array<struct<`pageid`:int,`timestamp`:bigint,`referer`:string>>,
  `request_set_token` string
  -- new fields must be added to end to match alter table behaviour
)
PARTITIONED BY (
  `year` int,
  `month` int,
  `day` int,
  `hour` int
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/query_clicks/hourly'
;

