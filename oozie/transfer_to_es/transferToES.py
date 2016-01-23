# A very primitive script to transfer from hive data files
# to ElasticSearch HTTP endpoint
from pyspark import SparkContext
from pyspark.sql import SQLContext
from optparse import OptionParser
import requests
import json
import logging
import subprocess

oparser = OptionParser()
oparser.add_option("-s", "--source", dest="source", help="source for the data", metavar="SOURCE")
oparser.add_option("-u", "--url", dest="url", help="URL to send the data to", metavar="URL")
oparser.add_option("-m", "--hostmap", dest="hostmap", help="Hostnames map in JSON",
    metavar="FILE", default="hostmap.json")
oparser.add_option("-b", "--batch", dest="batch", help="Items per batch to load into ES", metavar="NUM", default="10")
oparser.add_option("-n", "--noop-within", dest="noop",
    help="Only perform update if value has changed by more than this percentage",
    metavar="NOOP_WITHIN")
oparser.add_option("-f", "--field-name", dest="field", help="Name of elasticsearch field to populate",
    metavar="FIELD", default="score")

(options, args) = oparser.parse_args()

ITEMS_PER_BATCH = int(options.batch)
SOURCE = options.source
TARGET = options.url
NOOP_WITHIN = options.noop
FIELD = options.field
if options.hostmap[0:24] == 'hdfs://analytics-hadoop/':
    hostMap = json.loads(subprocess.check_output(["hdfs", "dfs", "-cat", options.hostmap[23:]]))
else:
    hostMap = json.load(open(options.hostmap))

print "Transferring from %s to %s" % (SOURCE, TARGET)

if __name__ == "__main__":
    sc = SparkContext(appName="Send To ES")
    sqlContext = SQLContext(sc)
    broardcastMap = sc.broadcast(hostMap)
    documentCounter = sc.accumulator(0)
    updateCounter = sc.accumulator(0)
    errorCounter = sc.accumulator(0)
    failedDocumentCounter = sc.accumulator(0)

    def documentData(document):
        """
        Create textual representation of the document data for one document
        """
        updateData = {"update": {"_id": document.page_id}}
        if NOOP_WITHIN:
            updateDoc = {"script": {
                "script": "super_detect_noop",
                "lang": "native",
                "params": {
                    "detectors": {FIELD: "within " + NOOP_WITHIN + "%"},
                    "source": {FIELD: document.score},
                },
            }}
        else:
            updateDoc = {"doc": {FIELD: document.score}}

        return json.dumps(updateData) + "\n" + json.dumps(updateDoc) + "\n"

    def getTargetURL(wikihost):
        """
        Get ES URL from wiki hostname
        """
        if wikihost in broardcastMap.value:
            wiki = broardcastMap.value[wikihost]
            return TARGET + "/" + wiki + "_content" + "/page/_bulk"
        return None

    def sendDataToES(data, url):
        """
        Send data to ES server
        """
        if len(data) < 1:
            return
        try:
            r = requests.put(url, data=data)
        except requests.exceptions.RequestException, e:
            errorCounter.add(1)
            logging.error("Failed to send update: " + str(e))
            return False
        if r.status_code != 200:
            errorCounter.add(1)
            r.close()
            return False
        parseResponse(r)
        updateCounter.add(1)
        return True

    def parseResponse(resp):
        respData = resp.json()
        for item in respData.get('items', {}):
            if 'update' not in item:
                continue
            if 'status' not in item['update']:
                continue
            if item['update']['status'] != 200:
                if 'error' in item['update'] and item['update']['error'][0:24] == 'DocumentMissingException':
                    continue
                failedDocumentCounter.add(1)

    def sendDocumentsToES(documents):
        """
        Send a set of documents to ES
        """
        if len(documents) < 1:
            return
        url = getTargetURL(documents[0].project)
        if not url:
            return
        data = ""
        for document in documents:
            data += documentData(document)
        documentCounter.add(len(documents))
        if not sendDataToES(data, url):
            failedDocumentCounter.add(len(documents))

    def addToList(listNow, element):
        """
        Add element to list, will send the data out once batch is full
        """
        if len(listNow) < ITEMS_PER_BATCH:
            return listNow + [element]
        # Treshold reached, send the list out
        sendDocumentsToES(listNow)
        return [element]

    def mergeLists(listOne, listTwo):
        """
        Merge two lists, will send the data out once batch is full
        """
        newList = listOne + listTwo
        if len(newList) < ITEMS_PER_BATCH:
            return newList
        sendDocumentsToES(newList)
        return []

    def sendCombined(data):
        """
        Send data for the key (hostname) to ES
        """
        sendDocumentsToES(data[1])

    data = sqlContext.load(SOURCE)
    # print "Count: %d\n" % data.count()
    # Here's what is going on here: we combine the data by project,
    # and when the list of projects reaches ITEMS_PER_BATCH we send them out
    # The foreach() part will send out the ones that remained
    data.map(lambda x: (x.project, x)).combineByKey(lambda x: [x], addToList, mergeLists).foreach(sendCombined)
    print "%d documents processed, %d failed." % (documentCounter.value, failedDocumentCounter.value,)
    print "%d requests successful, %d requests failed." % (updateCounter.value, errorCounter.value)
