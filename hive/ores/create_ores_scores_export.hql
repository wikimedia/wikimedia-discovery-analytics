CREATE TABLE `discovery`.`ores_scores_export` (
  `page_id` int COMMENT 'MediaWiki page id',
  `page_namespace` int COMMENT 'MediaWiki namespace page_id belongs to',
  `probability` map<string,float> COMMENT 'predicted classification as key, confidence as value'
)
PARTITIONED BY (
  `wikiid` string COMMENT 'Mediawiki database name',
  `model` string COMMENT 'ORES model that produced predictions',
  `year` int COMMENT 'Unpadded year topic collection starts at',
  `month` int COMMENT 'Unpadded month topic collection starts at',
  `day` int COMMENT 'Unpadded day topic collection starts at'
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/ores/scores_export_v2'
;
