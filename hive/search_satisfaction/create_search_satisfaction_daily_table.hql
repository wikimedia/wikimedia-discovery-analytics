CREATE EXTERNAL TABLE `discovery.search_satisfaction_daily`(
  `wiki` string COMMENT 'The wiki db name',
  `searchSessionId` string COMMENT 'A unique per-session identifier',
  `bucket` string COMMENT 'The test bucket this event belongs to',
  `sample_multiplier` float COMMENT 'A sampling correction factor giving the number of events one event should count as to account for 100% of unsampled events',

  `dt` string COMMENT 'ISO8601 Date and time the search was performed',
  `is_autorewrite_dym` boolean COMMENT 'True when query suggestion autorewrite was applied. This is a subset of is_dym, meaning if this is true is_dym is also true.',
  `is_dym` boolean COMMENT 'True when the query searched for is a suggested query',
  `dym_shown` boolean COMMENT 'True when a query suggestion was displayed at the top of the SERP',
  `dym_clicked` boolean COMMENT 'True when a query suggestion was clicked',
  `hits_returned` int COMMENT 'The total number of hits available for the search query',
  `hit_interact` boolean COMMENT 'True when a search result was clicked',

  `results_provider` string COMMENT 'Named provider of search result list',
  `sugg_provider` string COMMENT 'Named provider of search suggestion',

  `continent` string COMMENT '',
  `country_code` string COMMENT '',
  `country` string COMMENT '',
  `subdivision` string COMMENT '',
  `city` string COMMENT '',

  `ua_device_family` string COMMENT '',
  `ua_browser_family` string COMMENT '',
  `ua_browser_major` string COMMENT '',
  `ua_browser_minor` string COMMENT '',
  `ua_os_family` string COMMENT '',
  `ua_os_major` string COMMENT '',
  `ua_os_minor` string COMMENT '',
  `ua_is_bot` string COMMENT '',
  `ua_is_mediawiki` string COMMENT '',
  `ua_wmf_app_version` string COMMENT ''
)
PARTITIONED BY (
  `year` int COMMENT 'Unpadded year aggregated over',
  `month` int COMMENT 'Unpadded month aggregated over',
  `day` int COMMENT 'Unpadded day aggregated over')
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/discovery/search_satisfaction/daily_v02'
;
