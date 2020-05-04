# While this is stored as an hql file, these steps should be performed
# manually and verified after each step.


### Rename table to a temporary name.
ALTER TABLE `discovery.search_satisfaction_daily`
	RENAME TO `discovery.search_satisfaction_daily_migrate`;

### Run create_search_satisfaction_daily.hql
CREATE TABLE ...

### Copy over the data

# Allow partition selection on per-row basis
SET hive.exec.dynamic.partition.mode=nonstrict;
# Allow query with no partition predicate
SET hive.mapred.mode=unstrict;

INSERT OVERWRITE TABLE `discovery.search_satisfaction_daily`
	PARTITION(year, month, day)
SELECT
	wiki, searchSessionId, bucket, sample_multiplier, dt,
	is_autorewrite_dym, is_dym, dym_shown, dym_clicked,
	hits_returned, hit_interact, "n/a" as results_provider,
	"n/a" as sugg_provider, continent, country_code, country,
	subdivision, city, ua_device_family, ua_browser_family,
	ua_browser_major, ua_browser_minor, ua_os_family,
	ua_os_major, ua_os_minor, ua_is_bot, ua_is_mediawiki,
	ua_wmf_app_version, year, month, day
FROM `discovery.search_satisfaction_daily_migrate`;

### Drop the old data

DROP TABLE `discovery.search_satisfaction_daily_migrate`;


