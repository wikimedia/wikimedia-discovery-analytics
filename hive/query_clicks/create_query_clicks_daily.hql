CREATE TABLE `discovery.query_clicks_daily`(
  `query` string,
  `q_by_ip_day` int,
  `timestamp` bigint,
  `wikiid` string,
  `project` string,
  `hits` array<struct<title:string,index:string,pageid:int,score:float,profilename:string>>,
  `clicks` array<struct<pageid:int,timestamp:bigint,referer:string>>,
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
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/query_clicks/daily'
;

