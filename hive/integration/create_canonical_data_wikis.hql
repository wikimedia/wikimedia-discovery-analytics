CREATE TABLE `canonical_data`.`wikis`(
  `database_code` string,
  `domain_name` string,
  `database_group` string,
  `language_code` string,
  `language_name` string,
  `status` string,
  `visibility` string,
  `editability` string,
  `english_name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/user/hive/warehouse/canonical_data.db/wikis'
;
