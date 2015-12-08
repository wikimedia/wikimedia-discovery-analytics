CREATE EXTERNAL TABLE `popularity_score`(
  `project` string COMMENT 'Project name from requests hostname',
  `page_id` int COMMENT 'MediaWiki page_id within the project that the score is for',
  `score` double COMMENT 'Popularity score between 0 and 1')
PARTITIONED BY (
  `agg_days` int COMMENT 'Unpadded number of days aggregated over',
  `year` int COMMENT 'Unpadded year score aggregation starts at',
  `month` int COMMENT 'Unpadded month score aggregation starts at',
  `day` int COMMENT 'Unpadded day score aggregation starts at',
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/discovery/popularity_score'
