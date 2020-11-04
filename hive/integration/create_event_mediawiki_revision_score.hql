CREATE TABLE `event`.`mediawiki_revision_score`(
  `_schema` string,
  `meta` struct<uri:string,request_id:string,id:string,dt:string,domain:string,stream:string>,
  `database` string,
  `performer` struct<user_id:bigint,user_text:string,user_groups:array<string>,user_is_bot:boolean,user_registration_dt:string,user_edit_count:bigint>,
  `page_id` bigint,
  `page_title` string,
  `page_namespace` bigint,
  `page_is_redirect` boolean,
  `rev_id` bigint,
  `rev_parent_id` bigint,
  `rev_timestamp` string,
  `scores` map<string,struct<model_name:string,model_version:string,prediction:array<string>,probability:map<string,double>>>,
  `errors` map<string,struct<model_name:string,model_version:string,type:string,message:string>>)
PARTITIONED BY (
  `datacenter` string,
  `year` bigint,
  `month` bigint,
  `day` bigint,
  `hour` bigint)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs:///wmf/data/event/mediawiki_revision_score'
TBLPROPERTIES (
  'spark.sql.create.version'='2.2 or prior',
  'spark.sql.sources.schema.numPartCols'='5',
  'spark.sql.sources.schema.numParts'='1',
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"_schema\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"meta\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"uri\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"request_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dt\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"domain\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stream\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"database\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"performer\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"user_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"user_text\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"user_groups\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"user_is_bot\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"user_registration_dt\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"user_edit_count\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"page_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"page_title\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"page_namespace\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"page_is_redirect\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"rev_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"rev_parent_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"rev_timestamp\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"scores\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"model_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"model_version\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"prediction\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"probability\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"double\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}]},\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"errors\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"model_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"model_version\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"type\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"message\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"datacenter\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"year\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"month\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hour\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}',
  'spark.sql.sources.schema.partCol.0'='datacenter',
  'spark.sql.sources.schema.partCol.1'='year',
  'spark.sql.sources.schema.partCol.2'='month',
  'spark.sql.sources.schema.partCol.3'='day',
  'spark.sql.sources.schema.partCol.4'='hour')
