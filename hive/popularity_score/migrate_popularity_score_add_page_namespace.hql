# While this is stored as an hql file, these steps should be performed
# manually and verified after each step.


### Rename table to a temporary name.
ALTER TABLE `discovery.popularity_score`
    RENAME TO `discovery.popularity_score_migrate`;

### Run create_popularity_score.hql
CREATE TABLE ...

### Copy over the data

# Allow partition selection on per-row basis
SET hive.exec.dynamic.partition.mode=nonstrict;
# Allow query with no partition predicate
SET hive.mapred.mode=unstrict;

INSERT OVERWRITE TABLE `discovery.popularity_score`
    PARTITION(agg_days, year, month, day)
SELECT
    project, page_id, NULL, score,
    agg_days, year, month, day
FROM `discovery.popularity_score_migrate`;

### Drop the old data

DROP TABLE `discovery.popularity_score_migrate`;
