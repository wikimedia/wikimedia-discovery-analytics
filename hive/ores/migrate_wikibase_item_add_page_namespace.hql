# While this is stored as an hql file, these steps should be performed
# manually and verified after each step.


### Rename table to a temporary name.
ALTER TABLE `discovery.ores_wikibase_item`
    RENAME TO `discovery.ores_wikibase_item_migrate`;

### Run create_ores_wikibase_item.hql
CREATE TABLE ...

### Copy over the data

# Allow partition selection on per-row basis
SET hive.exec.dynamic.partition.mode=nonstrict;
# Allow query with no partition predicate
SET hive.mapred.mode=unstrict;

INSERT OVERWRITE TABLE `discovery.ores_wikibase_item`
    PARTITION(year, month, day)
SELECT
    wikiid, page_id, NULL, wikibase_item,
    year, month, day
FROM `discovery.ores_wikibase_item_migrate`;

### Drop the old data

DROP TABLE `discovery.ores_wikibase_item_migrate`;