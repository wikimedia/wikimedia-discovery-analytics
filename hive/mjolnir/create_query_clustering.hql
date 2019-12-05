CREATE TABLE `mjolnir.query_clustering` (
  `wikiid` string,
  `query` string,
  `cluster_id` bigint
)
PARTITIONED BY (
  `date` string,
  `algorithm` string
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/mjolnir/query_clustering'
;
