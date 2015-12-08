# Aggregation phase for discovery popularity score from pageview hourly

This job is responsible for aggregating the weeks worth
of pageview data from the pageview_hourly table, aggregating
it into a percentage of pageviews to the project for each
page.

Output is appended into (agg_days, year, month, day) partitions
for the first day of the week calculated in
/wmf/data/wmf/discovery/popularity_score

# Outline

* ```coordinator.properties``` is used to define parameters to the
  aggregation pipeline.
* ```coordinator.xml``` injects the aggregation workflow for each dataset.
* ```workflow.xml```
  * Runs a spark app to aggregate from pageview into discovery_popularity_score

Note that this job uses the pageview_hourly dataset.  If a pageview
aggregation job does not have the _SUCCESS done-flag in the directory,
the data for that week will not be aggregated until it does.

@TODO is that desirable? We can probably get reasonable results if an
hour or two from the week is missing
