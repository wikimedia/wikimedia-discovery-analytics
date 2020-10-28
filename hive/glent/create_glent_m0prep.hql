CREATE TABLE `glent`.`m0prep` (
  `q1_query` string,
  `q1_queryNorm` string,
  `q1_wikiid` string,
  `q1_lang` string,
  `q2_query` string,
  `q2_queryNorm` string,
  `q2_wikiid` string,
  `q2_lang` string,
  `q1_ts` int,
  `q1_hitsTotal` int,
  `q2_hitsTotal` int,
  `q1q2LevenDist` int,
  `suggCount` int
)
PARTITIONED BY (
  `date` string
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/discovery/glent/m0prep'
;
