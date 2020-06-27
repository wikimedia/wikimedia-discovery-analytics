CREATE TABLE `discovery.wikibase_item` (
  `wikiid` string COMMENT 'MediaWiki database name',
  `page_id` int COMMENT 'MediaWiki page id',
  `page_namespace` int COMMENT 'MediaWiki namespace page_id belongs to',
  `wikibase_item` string COMMENT 'wikibase_item page property from mediawiki database'
)
PARTITIONED BY (
  `year` int COMMENT 'Unpadded year of data extraction task execution date',
  `month` int COMMENT 'Unpadded month of data extraction task execution date',
  `day` int COMMENT 'Unpadded day of data extraction task execution date'
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/ores/wikibase_item_v2'
;
