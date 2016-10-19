CREATE EXTERNAL TABLE `popularity_score`(
  `project` string COMMENT 'Project name from requests hostname',
  `page_id` int COMMENT 'MediaWiki page_id within the project that the score is for',
  `score` double COMMENT 'Popularity score between 0 and 1')
PARTITIONED BY (
  `agg_days` int COMMENT 'Unpadded number of days aggregated over',
  `year` int COMMENT 'Unpadded year score aggregation ends at',
  `month` int COMMENT 'Unpadded month score aggregation ends at',
  `day` int COMMENT 'Unpadded day score aggregation ends at')
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/discovery/popularity_score'
