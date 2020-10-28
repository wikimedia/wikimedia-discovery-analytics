CREATE TABLE `glent`.`m1prep` (
  `query` string,
  `queryNorm` string,
  `wikiid` string,
  `lang` string,
  `ts` int,
  `hitsTotal` int,
  `suggCount` int
)
PARTITIONED BY (
  `date` string
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/discovery/glent/m1prep'
;
