CREATE EXTERNAL TABLE `event`.`SearchSatisfaction`(
  `dt` string,
  `event` struct<`action`:string,`articleId`:bigint,`autocompleteType`:string,`hitsReturned`:bigint,`inputLocation`:string,`msToDisplayResults`:bigint,`mwSessionId`:string,`pageViewId`:string,`query`:string,`scroll`:boolean,`searchSessionId`:string,`source`:string,`subTest`:string,`uniqueId`:string,`position`:bigint,`didYouMeanVisible`:string,`extraParams`:string,`searchToken`:string,`checkin`:bigint,`isForced`:boolean,`sampleMultiplier`:double,`skin`:string,`skinVersion`:string,`isAnon`:boolean>,
  `recvfrom` string,
  `revision` bigint,
  `schema` string,
  `seqid` bigint,
  `useragent` struct<`browser_family`:string,`browser_major`:string,`browser_minor`:string,`device_family`:string,`is_bot`:boolean,`is_mediawiki`:boolean,`os_family`:string,`os_major`:string,`os_minor`:string,`wmf_app_version`:string>,
  `uuid` string,
  `webhost` string,
  `wiki` string,
  `ip` string,
  `geocoded_data` map<string,string>,
  `topic` string,
  `_schema` string,
  `client_dt` string,
  `http` struct<`method`:string,`status_code`:bigint,`client_ip`:string,`has_cookies`:boolean,`request_headers`:map<string,string>,`response_headers`:map<string,string>>,
  `meta` struct<`uri`:string,`request_id`:string,`id`:string,`dt`:string,`domain`:string,`stream`:string>,
  `user_agent_map` map<string,string>)
PARTITIONED BY (
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
  'hdfs://analytics-hadoop/wmf/data/event/SearchSatisfaction'
TBLPROPERTIES (
  'spark.sql.create.version'='2.2 or prior',
  'spark.sql.sources.schema.numPartCols'='4',
  'spark.sql.sources.schema.numParts'='2',
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"dt\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"event\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"action\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"articleId\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"autocompleteType\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hitsReturned\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"inputLocation\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"msToDisplayResults\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"mwSessionId\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"pageViewId\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"query\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"scroll\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"searchSessionId\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"source\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"subTest\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"uniqueId\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"position\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"didYouMeanVisible\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"extraParams\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"searchToken\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"checkin\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"isForced\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"sampleMultiplier\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"skin\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"skinVersion\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"isAnon\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"recvfrom\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"revision\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"schema\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"seqid\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"useragent\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"browser_family\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"browser_major\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"browser_minor\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"device_family\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"is_bot\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"is_mediawiki\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"os_family\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"os_major\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"os_minor\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"wmf_app_version\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"uuid\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"webhost\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"wiki\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ip\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"geocoded_data\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"topic\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_schema\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"client_dt\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"http\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"method\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"status_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"client_ip\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"has_cookies\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"request_headers\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"response_headers\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullabl',
  'spark.sql.sources.schema.part.1'='e\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"meta\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"uri\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"request_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dt\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"domain\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stream\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"user_agent_map\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"year\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"month\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hour\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}',
  'spark.sql.sources.schema.partCol.0'='year',
  'spark.sql.sources.schema.partCol.1'='month',
  'spark.sql.sources.schema.partCol.2'='day',
  'spark.sql.sources.schema.partCol.3'='hour',
  'transient_lastDdlTime'='1603116984')
;
