CREATE TABLE `discovery.cirrus_namespace_index_map` (
    `wikiid` string COMMENT 'MediaWiki database name',
    `namespace_id` int COMMENT 'MediaWiki namespace id',
    `elastic_index` string COMMENT 'CirrusSearch index containing namespace_id'
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/cirrus_namespace_index_map'
;
