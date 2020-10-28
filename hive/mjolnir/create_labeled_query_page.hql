CREATE TABLE `mjolnir`.`labeled_query_page` (
  `wikiid` string,
  `query` string,
  `page_id` int,
  `label` double COMMENT 'Meaning of a label depends on the algorithm. For DBN this is a value between 0 and 1',
  `cluster_id` bigint COMMENT 'Labels with the same cluster_id are not independent in some algorithms (DBN). TODO: Better name?'
)
PARTITIONED BY (
  `date` string,
  `algorithm` string
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/mjolnir/labeled_query_page'
;
