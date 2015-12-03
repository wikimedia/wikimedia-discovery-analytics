# A very primitive script to transfer from hive data files
# to ElasticSearch HTTP endpoint
from pyspark import SparkContext
from pyspark.sql import SQLContext
from optparse import OptionParser
import json

oparser = OptionParser()
oparser.add_option("-s", "--source", dest="source", help="Source for the data", metavar="SOURCE")
oparser.add_option("-w", "--wiki", dest="wiki", help="Wiki name to export", metavar="NAME")
oparser.add_option("-t", "--target", dest="target", help="Directory name to use", metavar="TARGET")

(options, args) = oparser.parse_args()

if __name__ == "__main__":
    sc = SparkContext(appName="Send To ES")
    sqlContext = SQLContext(sc)

    def documentData(document):
        """
        Create textual representation of the document data for one document
        """
        updateData = {"update": {"_id": document.page_id}}
        updateDoc = {"doc": {"score": document.score}}
        return json.dumps(updateData) + "\n" + json.dumps(updateDoc)

    data = sqlContext.load(options.source)
    data.filter(data.project == options.wiki).map(documentData).saveAsTextFile(options.target)
