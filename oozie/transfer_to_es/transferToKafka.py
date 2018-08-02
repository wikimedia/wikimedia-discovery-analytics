# A very primitive script to transfer from hive data files
# to ElasticSearch HTTP endpoint
from __future__ import print_function
try:
    from pyspark import SparkContext
except ImportError:
    import findspark
    findspark.init()
    from pyspark import SparkContext
from pyspark.sql import SQLContext
from optparse import OptionParser
import json
import kafka
import subprocess
import time

oparser = OptionParser()
oparser.add_option("-s", "--source", dest="source", help="source for the data", metavar="SOURCE")
oparser.add_option("-k", "--kafka-brokers", dest="brokers", metavar="URL", default="localhost:9092",
    help="Kafka brokers to bootstrap from as a comma separated list of <host>:<port>")
oparser.add_option('-t', '--topic', dest='topic', help="Kafka topic to produce into", metavar='TOPIC')
oparser.add_option("-m", "--hostmap", dest="hostmap", help="Hostnames map in JSON",
    metavar="FILE", default="hostmap.json")
oparser.add_option("-f", "--field-name", dest="field", help="Name of elasticsearch field to populate",
    metavar="FIELD", default="score")
oparser.add_option("-r", "--rate-limit", dest="rate", help="Maximum messages/s per producer")

(options, args) = oparser.parse_args()

SOURCE = options.source
BROKERS = options.brokers.split(',')
TOPIC = options.topic
if not TOPIC:
    raise RuntimeError("...")
FIELD = options.field
RATE = int(options.rate)
if options.hostmap[0:24] == 'hdfs://analytics-hadoop/':
    hostMap = json.loads(subprocess.check_output(["hdfs", "dfs", "-cat", options.hostmap[23:]]))
else:
    hostMap = json.load(open(options.hostmap))


def ratelimit(rows):
    now = time.monotonic if hasattr(time, 'monotonic') else time.time
    next_reset = now() + 1
    num_rows = 0
    for row in rows:
        time_remaining = next_reset - now()
        if time_remaining < 0 or num_rows >= RATE:
            if time_remaining > 0:
                time.sleep(time_remaining)
            num_rows = 0
            next_reset = now() + 1
        num_rows += 1
        yield row


print("Transferring from %s to %s" % (SOURCE, BROKERS[0]))

if __name__ == "__main__":
    sc = SparkContext(appName="Send To ES: %s" % (BROKERS[0]))
    sqlContext = SQLContext(sc)
    broadcastMap = sc.broadcast(hostMap)
    documentCounter = sc.accumulator(0)
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
                                       client_id='transfer-to-cirrussearch',
                                       compression_type='gzip',
                                       api_version=(0, 9))
        try:
            for row in ratelimit(rows):
                producer.send(TOPIC, row.encode('utf-8'))
                documentCounter.add(1)
        finally:
            producer.close()

    # Ideally we want to produce each wiki one after the other so when the data
    # is consumed it creates segments in a few indices at a time instead of
    # hitting all indices in a random order. Sorting the data by project
    # requires deserializing the whole thing into memory at once, so instead
    # work through the list one project at a time. parqet page stats will ensure
    # we don't load most of the data for the hundreds of tiny projects.
    df = sqlContext.read.parquet(SOURCE)
    all_projects = [r.project for r in df.select(df['project']).drop_duplicates().collect()]
    for project in all_projects:
        print("Processing %s" % (project))
        df.where(df['project'] == project) \
            .rdd.flatMap(documentData) \
            .foreachPartition(send)

    print("%d documents processed, %d failed." % (documentCounter.value, failedDocumentCounter.value))
