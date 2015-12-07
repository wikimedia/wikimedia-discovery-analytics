# Transfer phase of discovery popularity score to elasticsearch clusters

This job is reposible for transfering popularity score information from
the hadoop cluster to the elasticsearch cluster.

# Outline

* ```bundle.properties``` is used to define parameters to the pipeline
* ```bundle.xml``` Defines a separate import job for each active cluster
* ```coordinator.xml``` Defines when and how to run the import job
* ```workflow.xml```
  * Runs a spark app to read in the scoring information from 
    discovery.popularity_score and write it to the specified elasticsearch
    URL.

Note that this job uses the popularity_score dataset. If the aggregation job
does not have the _SUCCESS done-flag in the directory, the scores for that
week will not be transfered until it does.
