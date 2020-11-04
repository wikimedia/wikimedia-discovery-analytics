CREATE TABLE `event`.`mediawiki_cirrussearch_request`(
  `_schema` string,
  `meta` struct<`uri`:string,`request_id`:string,`id`:string,`dt`:string,`domain`:string,`stream`:string>,
  `http` struct<`method`:string,`client_ip`:string,`request_headers`:map<string,string>,`has_cookies`:boolean>,
  `database` string,
  `mediawiki_host` string,
  `params` map<string,string>,
  `search_id` string,
  `source` string,
  `identity` string,
  `backend_user_tests` array<string>,
  `request_time_ms` bigint,
  `hits` array<struct<`page_title`:string,`page_id`:bigint,`index`:string,`score`:double,`profile_name`:string>>,
  `all_elasticsearch_requests_cached` boolean,
  `elasticsearch_requests` array<struct<`query`:string,`query_type`:string,`indices`:array<string>,`namespaces`:array<bigint>,`request_time_ms`:bigint,`search_time_ms`:bigint,`limit`:bigint,`hits_total`:bigint,`hits_returned`:bigint,`hits_offset`:bigint,`suggestion`:string,`suggestion_requested`:boolean,`max_score`:double,`langdetect`:string,`syntax`:array<string>,`cached`:boolean,`hits`:array<struct<`page_title`:string,`page_id`:bigint,`index`:string,`score`:double,`profile_name`:string>>>>,
  `geocoded_data` map<string,string>,
  `user_agent_map` map<string,string>)
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
  'hdfs://analytics-hadoop/wmf/data/event/mediawiki_cirrussearch_request'
TBLPROPERTIES (
  'last_modified_by'='analytics',
  'last_modified_time'='1588267276',
  'spark.sql.create.version'='2.2 or prior',
  'spark.sql.sources.schema.numPartCols'='5',
  'spark.sql.sources.schema.numParts'='2',
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"_schema\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"meta\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"uri\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"request_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dt\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"domain\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stream\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"http\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"method\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"client_ip\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"request_headers\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"has_cookies\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"database\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"mediawiki_host\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"params\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"search_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"source\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"identity\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"backend_user_tests\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"request_time_ms\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hits\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"page_title\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"page_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"index\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"score\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"profile_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"all_elasticsearch_requests_cached\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"elasticsearch_requests\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"query\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"query_type\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"indices\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"namespaces\",\"type\":{\"type\":\"array\",\"elementType\":\"long\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"request_time_ms\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"search_time_ms\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"limit\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hits_total\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hits_returned\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hits_offset\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"suggestion\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"suggestion_requested\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"max_score\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"langdetect\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"syntax\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"cached\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hits\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"page_title\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"page_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"index\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"score\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"profile_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]},\"containsNull',
  'spark.sql.sources.schema.part.1'='\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"geocoded_data\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"user_agent_map\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"datacenter\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"year\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"month\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hour\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}',
  'spark.sql.sources.schema.partCol.0'='datacenter',
  'spark.sql.sources.schema.partCol.1'='year',
  'spark.sql.sources.schema.partCol.2'='month',
  'spark.sql.sources.schema.partCol.3'='day',
  'spark.sql.sources.schema.partCol.4'='hour')