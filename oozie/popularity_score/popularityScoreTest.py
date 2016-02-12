import unittest
import json
import pyspark
import pyspark.sql

from popularityScore import calcPopularityScore


class PopularityScoreTest(unittest.TestCase):
    def setUp(self):
        self.sc = pyspark.SparkContext()
        self.sql = pyspark.sql.SQLContext(self.sc)

    def createDataFrame(self, fixture):
        encoded = [json.dumps(item) for item in fixture]
        return self.sql.jsonRDD(self.sc.parallelize(encoded))

    def test_foo(self):
        fixture = self.createDataFrame([
            {
                'project': 'en.wikipedia',
                'page_id': 12345,
                'view_count': 1,
            },
            {
                'project': 'en.wikipedia',
                'page_id': 12345,
                'view_count': 5,

            },
            {
                'project': 'en.wikipedia',
                'page_id': None,
                'view_count': 5,
            },
        ])

        result = [row.asDict() for row in calcPopularityScore(self.sc, fixture).collect()]
        self.assertEqual(result, [
            {
                u'project': 'en.wikipedia',
                u'page_id': 12345,
                u'score': 1.0,
            }
        ])

if __name__ == '__main__':
    unittest.main()
