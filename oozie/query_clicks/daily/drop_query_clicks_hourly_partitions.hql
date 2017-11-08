-- Paremeters:
--   source_table -- Table to drop hourly partitions from
--   year         -- Year of partitions to drop
--   month        -- Month of partitions to drop
--   day          -- Day of partitions to drop

ALTER TABLE
    ${source_table}
DROP PARTITION
    (year=${year}, month=${month}, day=${day}, hour>=0)
;
