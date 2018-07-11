# A very primitive script to transfer from hive data files
# to ElasticSearch HTTP endpoint
from __future__ import print_function
try:
    from pyspark import SparkContext
except ImportError:
    import findspark
    findspark.init()
    from pyspark import SparkContext
from pyspark.sql import functions as F, SQLContext
from optparse import OptionParser
import json
import kafka
import subprocess

oparser = OptionParser()
oparser.add_option("-s", "--source", dest="source", help="source for the data", metavar="SOURCE")
oparser.add_option("-k", "--kafka-brokers", dest="brokers", metavar="URL", default="localhost:9092",
    help="Kafka brokers to bootstrap from as a comma separated list of <host>:<port>")
oparser.add_option('-t', '--topic', dest='topic', help="Kafka topic to produce into", metavar='TOPIC')
oparser.add_option("-m", "--hostmap", dest="hostmap", help="Hostnames map in JSON",
    metavar="FILE", default="hostmap.json")
oparser.add_option("-n", "--noop-within", dest="noop",
    help="Only perform update if value has changed by more than this percentage",
    metavar="NOOP_WITHIN")
oparser.add_option("-f", "--field-name", dest="field", help="Name of elasticsearch field to populate",
    metavar="FIELD", default="score")

(options, args) = oparser.parse_args()

SOURCE = options.source
BROKERS = options.brokers.split(',')
TOPIC = options.topic
if not TOPIC:
    raise RuntimeError("...")
NOOP_WITHIN = options.noop
FIELD = options.field
if options.hostmap[0:24] == 'hdfs://analytics-hadoop/':
    hostMap = json.loads(subprocess.check_output(["hdfs", "dfs", "-cat", options.hostmap[23:]]))
else:
    hostMap = json.load(open(options.hostmap))

print("Transferring from %s to %s" % (SOURCE, BROKERS[0]))

if __name__ == "__main__":
    sc = SparkContext(appName="Send To ES: %s" % (BROKERS[0]))
    sqlContext = SQLContext(sc)
    broadcastMap = sc.broadcast(hostMap)
    documentCounter = sc.accumulator(0)
    updateCounter = sc.accumulator(0)
    errorCounter = sc.accumulator(0)
    failedDocumentCounter = sc.accumulator(0)

    def getTargetWiki(wikihost):
        """
        Get ES URL from wiki hostname
        """
        if wikihost in broadcastMap.value:
            return broadcastMap.value[wikihost]
        return None

    def getTargetIndex(wiki):
        return '%s_content' % (wiki)

    def documentData(document):
        """
        Create textual representation of the document data for one document
        """
        wiki = getTargetWiki(document.project)
        if not wiki:
            failedDocumentCounter.add(1)
            return

        updateData = {
            "_index": getTargetIndex(wiki),
            "_id": document.page_id,
            "_source": {
                FIELD: document.score,
            }
        }

        yield json.dumps(updateData)

    def send(rows):
        """
        Pipe out the data to kafka
        """
        producer = kafka.KafkaProducer(bootstrap_servers=BROKERS,
                                       compression_type='gzip',
                                       api_version=(0, 9))
        try:
            for row in rows:
                producer.send(TOPIC, row.encode('utf-8'))
                documentCounter.add(1)
        finally:
            producer.close()

    sqlContext.read.json(SOURCE) \
        .sort(F.col('project')) \
        .rdd.flatMap(documentData) \
        .foreachPartition(send)

    print("%d documents processed, %d failed." % (documentCounter.value, failedDocumentCounter.value,))
