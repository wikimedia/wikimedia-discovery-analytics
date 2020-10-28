CREATE TABLE `mjolnir`.`feature_vectors` (
  `wikiid` string,
  `query` string,
  `page_id` int,
  `features` struct<`type`:tinyint,`size`:int,`indices`:array<int>,`values`:array<double>>
)
PARTITIONED BY (
  `date` string,
  `feature_set` string
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/mjolnir/feature_vectors'
;
