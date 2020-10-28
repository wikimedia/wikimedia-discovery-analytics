CREATE TABLE `discovery`.`wikidata_rdf` (
  `context` string,
  `subject` string,
  `predicate` string,
  `object` string
)
PARTITIONED BY (`date` string)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/wikidata/rdf/';
