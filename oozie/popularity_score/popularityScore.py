import pyspark
import pyspark.sql
import pyspark.sql.functions
import pyspark.sql.types
import argparse
import datetime
import subprocess

# Developed for spark 1.3.0
# Example testing use:
#   spark-submit --master yarn --deploy-mode client --driver-memory 2g \
#       --num-executors 48 --executor-cores 2 --executor-memory 4g \
#       popularityScore.py 2015/11/11 2015/11/18


def parse_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y/%m/%d")
    except ValueError:
        msg = "Not a valid date, must be in format YYYY/MM/DD: '{s}'" % {s: s}
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser()
parser.add_argument('--source-dir', dest='source_dir',
                    default='hdfs://analytics-hadoop/wmf/data/wmf/pageview/hourly',
                    help='Directory containing the wmf.pageview_hourly hive table')
parser.add_argument('--output-dir', dest='output_dir',
                    default='hdfs://analytics-hadoop/wmf/data/wmf/discovery/popularity_score',
                    help='Directory to write results out to including all partition directories ')
parser.add_argument('start_date', type=parse_date,
                    help='Inclusive date to start aggregation from')
parser.add_argument('end_date', type=parse_date,
                    help='Exclusive date to end aggregation on')


def pathForPageViewHourly(source, date):
    """Transform a base path + date into directory name
    partitioned by year, month, day, hour"""
    return "%(source)s/year=%(year)s/month=%(month)s/day=%(day)s/hour=%(hour)s" % {
        "source": source,
        "year": date.year,
        "month": date.month,
        "day": date.day,
        "hour": date.hour,
    }


def pageViewHourlyPathList(source, startDate, endDate):
    """Transform a base path plus start and end dates into a
    list of directories to read"""
    curDate = startDate
    delta = datetime.timedelta(hours=1)
    paths = []
    while curDate < endDate:
        paths.append(pathForPageViewHourly(source, curDate))
        curDate = curDate + delta
    return paths


def deleteHdfsDir(path):
    """Deletes a directory from HDFS. PySpark doesn't ship with any
    hdfs integration so this shells out"""
    subprocess.call(["hdfs", "dfs", "-rm", "-r", "-f", path])


def calcProjectPageViews(dataFrame):
    """Generates a dict from project name to the number of page
    views in the dataFrame for that project. For WMF this should
    generate a dict with less than 1000 entries"""
    data = dataFrame.groupBy(dataFrame.project).agg(
        dataFrame.project,
        pyspark.sql.functions.sum(dataFrame.view_count).alias("view_count"),
    ).collect()

    return {item.project: item.view_count for item in data}


def calcPopularityScore(sc, source):
    filtered = source.filter(
        source.page_id.isNotNull()
    )

    aggregated = filtered.groupBy(
        source.page_id,
        source.project,
    ).agg(
        source.project,
        source.page_id,
        pyspark.sql.functions.sum(source.view_count).alias("view_count"),
    )

    projectPageViews = sc.broadcast(calcProjectPageViews(filtered))
    # This is a very naive version of the popularity score, likely it will be extended over
    # time to be more robust. For the initial iterations this should be sufficient though.
    popularityScore = pyspark.sql.functions.udf(
        lambda view_count, project: view_count / float(projectPageViews.value[project]),
        pyspark.sql.types.DoubleType(),
    )

    print("Calculating popularity score")
    return aggregated.select(
        aggregated.project,
        aggregated.page_id,
        popularityScore(
            aggregated.view_count,
            aggregated.project
        ).alias('score'),
    )

if __name__ == "__main__":
    args = parser.parse_args()
    sc = pyspark.SparkContext(appName="Discovery Popularity Score")
    sqlContext = pyspark.sql.SQLContext(sc)

    parquetPaths = pageViewHourlyPathList(args.source_dir, args.start_date, args.end_date)
    print("loading pageview data from:")
    print("\t" + "\n\t".join(parquetPaths) + "\n")
    dataFrame = sqlContext.parquetFile(*parquetPaths)
    result = calcPopularityScore(sc, dataFrame)

    deleteHdfsDir(args.output_dir)
    # the default spark.sql.shuffle.partitions creates 200 partitions, resulting in 3mb files.
    # repartition to achieve result files close to 256mb (our default hdfs block size)
    print("Writing results to " + args.output_dir)
    # In pyspark 1.4 this can be updated to use coalesce which is more performant
    result.repartition(16).saveAsParquetFile(args.output_dir)
