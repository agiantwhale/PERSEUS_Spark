import unittest
from src.LocalCommunity import LocalCommunity
from pyspark.context import SparkContext


class LocalCommunityTest(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext('local[4]')
        self.graph = [
            (2, 1, 0.1),
            (2, 3, 0.2),
            (2, 5, 0.3),
            (2, 6, 0.4),
            (3, 1, 0.5),
            (3, 4, 0.6),
            (3, 6, 0.7),
            (4, 2, 0.8),
            (5, 3, 0.9)
        ]

    def tearDown(self):
        self.sc.stop()

    def test_local_community(self):
        lc = LocalCommunity()
        rdd = self.sc.parallelize(self.graph)
        result = lc.find_local_community(rdd)
        output = result.collectAsMap()

        correct = {
            1: [1, 2, 3],
            2: [1, 2, 3, 4, 5, 6],
            3: [1, 2, 3, 4, 5, 6],
            4: [2, 3, 4],
            5: [2, 3, 5],
            6: [2, 3, 6]
        }

        self.assertEqual(correct.keys(), output.keys())

        for k, v in output.iteritems():
            self.assertEqual(correct[k], sorted(list(v)))

    def test_neighbors(self):
        lc = LocalCommunity()
        rdd = self.sc.parallelize(self.graph)
        result = lc.find_neighbors(rdd)
        output = result.collectAsMap()

        correct = {
            1: [4, 5, 6],
            4: [1, 5, 6],
            5: [1, 4, 6],
            6: [1, 4, 5]
        }

        self.assertEqual(correct.keys(), output.keys())

        for k, v in output.iteritems():
            self.assertEqual(correct[k], sorted(list(v)))

    def test_total_local_community_weight(self):
        lc = LocalCommunity()
        rdd = self.sc.parallelize(self.graph)

        inbound_result = lc.find_total_local_community_weight(rdd, type="inbound")
        inbound_output = inbound_result.collectAsMap()

        inbound_correct = {
            1: 0.8 + 0.9,
            4: 0.9,
            5: 0.8,
            6: 0.8 + 0.9
        }

        self.assertEqual(inbound_correct.keys(), inbound_output.keys())

        for k, v in inbound_output.iteritems():
            self.assertAlmostEqual(inbound_correct[k], v)

        outbound_result = lc.find_total_local_community_weight(rdd, type="outbound")
        outbound_output = outbound_result.collectAsMap()

        outbound_correct = {
            1: 0.6 + 0.3 + 0.7 + 0.4,
            4: 0.1 + 0.5 + 0.4 + 0.7 + 0.3,
            5: 0.1 + 0.5 + 0.6 + 0.4 + 0.7,
            6: 0.1 + 0.5 + 0.6 + 0.3
        }

        self.assertEqual(outbound_correct.keys(), outbound_output.keys())

        for k, v in outbound_output.iteritems():
            self.assertAlmostEqual(outbound_correct[k], v)

if __name__ == "__main__":
    unittest.main()