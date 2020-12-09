CREATE TABLE `event`.`mediawiki_revision_recommendation_create`(
  `_schema` string,
  `meta` struct<uri:string,request_id:string,id:string,dt:string,domain:string,stream:string>,
  `database` string,
  `page_id` bigint,
  `page_title` string,
  `page_namespace` bigint,
  `rev_id` bigint,
  `rev_timestamp` string,
  `rev_sha1` string,
  `rev_minor_edit` boolean,
  `rev_len` int,
  `rev_content_model` string,
  `rev_content_format` string,
  `performer` struct<user_id:bigint,user_text:string,user_groups:array<string>,user_is_bot:boolean,user_registration_dt:string,user_edit_count:bigint>,
  `page_is_redirect` boolean,
  `rev_parent_id` bigint,
  `rev_content_changed` boolean,
  `recommendation_type` string)
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
  'hdfs:///wmf/data/event/mediawiki_revision_recommendation_create'
