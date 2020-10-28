CREATE TABLE `discovery`.`fulltext_head_queries` (
  `wiki` string,
  `norm_query` string,
  `rank` int,
  `num_sessions` int,
  `queries` array<struct<
    `query`: string,
    `num_sessions`: int>>
)
PARTITIONED BY (
  `date` string
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/fulltext_head_queries'
;

