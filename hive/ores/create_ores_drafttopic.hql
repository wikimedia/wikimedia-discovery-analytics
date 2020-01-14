CREATE TABLE `discovery.ores_drafttopic` (
  `wikiid` string COMMENT 'MediaWiki database name',
  `page_id` int COMMENT 'MediaWiki page id',
  `drafttopic` array<string> COMMENT 'topics formatted as name|int_score'
)
PARTITIONED BY (
  `year` int COMMENT 'Unpadded year topic collection starts at',
  `month` int COMMENT 'Unpadded month topic collection starts at',
  `day` int COMMENT 'Unpadded day topic collection starts at'
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/ores/drafttopic'
;
