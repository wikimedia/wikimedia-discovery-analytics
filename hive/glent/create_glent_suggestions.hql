CREATE TABLE `glent`.`suggestions` (
  `query` string,
  `dym` string,
  `suggCount` int,
  `q1q2EditDist` float,
  `queryHitsTotal` int,
  `dymHitsTotal` int,
  `wikiid` string,
  `lang` string,
  `ts` int
)
PARTITIONED BY (
  `algo` string,
  `date` string
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/discovery/glent/suggestions_v2'
;
