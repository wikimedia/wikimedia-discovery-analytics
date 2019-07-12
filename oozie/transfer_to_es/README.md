# Transfer phase of discovery popularity score to elasticsearch clusters

This job is reposible for transforming the popularity score dataset into
elasticsearch updates and uploading them to swift. It further notifies 
a kafka topic about the availability of these new files which is expected
expected to be read and imported by a MjoLniR kafka bulk daemon.

# Outline

* ```coordinator.properties``` is used to define parameters to the pipeline
* ```coordinator.xml``` Defines when and how to run the import job
* ```workflow.xml```
  * Runs a spark app to read in the scoring information from 
    discovery.popularity_score and write it to the provided directory
  * Runs a sub-workflow to upload the output directory to swift
  * TODO: Notify kafka (probably in the sub-workflow?)

Note that this job uses the popularity_score dataset. If the aggregation job
does not have the _SUCCESS done-flag in the directory, the scores for that
week will not be transfered until it does.
