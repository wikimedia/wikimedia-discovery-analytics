CREATE TABLE `discovery.ores_drafttopic` (
  `wikiid` string COMMENT 'MediaWiki database name',
  `page_id` int COMMENT 'MediaWiki page id',
  `page_namespace` int COMMENT 'MediaWiki namespace page_id belongs to',
  `drafttopic` array<string> COMMENT 'ores drafttopic predictions formatted as name|int_score for elasticsearch ingestion'
)
PARTITIONED BY (
  `year` int COMMENT 'Unpadded year topic collection starts at',
  `month` int COMMENT 'Unpadded month topic collection starts at',
  `day` int COMMENT 'Unpadded day topic collection starts at'
)
STORED AS PARQUET
LOCATION 'hdfs:///wmf/data/discovery/ores/drafttopic'
;
