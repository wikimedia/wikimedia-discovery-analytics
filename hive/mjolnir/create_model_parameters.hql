-- Each row is a single set of model parameters tested or trained.
CREATE TABLE `mjolnir.model_parameters` (
  `run_id` string,
  `parent_run_id` string,
  `started_at` timestamp,
  `completed_at` timestamp,
  -- these three could fit generically into metrics/params,
  -- but convenient to have at top level
  `algorithm` string,
  `objective` string,
  `loss` double,
  -- stringifying numeric params since we don't have union
  `params` map<string, string>,
  -- basic information about data that was trained against
  `folds` array<struct<
    wikiid: string,
    vec_format: string,
    split_name: string,
    path: string,
    fold_id: int
  >>,
  `metrics` array<struct<
    key: string,
    value: double,
    step: int,
    fold_id: int,
    split: string
  >>,
  -- artifact name to hdfs path
  `artifacts` map<string, string>
)
PARTITIONED BY (
  `date` string,
  `wikiid` string,
  `labeling_algorithm` string,
  `feature_set` string
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/wmf/data/discovery/mjolnir/model_parameters'
;
