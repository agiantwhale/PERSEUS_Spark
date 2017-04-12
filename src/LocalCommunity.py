'''
Created on April 9, 2017

@author: Il Jae Lee (iljae@umich.edu)
'''

from operator import add

class LocalCommunity:

    def __init__(self):
        self._descriotion = 'In local community'

    def find_local_community(self, D):
        """
        Finds the local community of each node
        :param D: PySpark data object
        :return: PySpark RDD object
        """
        # Disregard weight
        graph = D.map(lambda r: (r[0], r[1]))
        # Local community disregards order
        rdd = graph.union(graph.map(lambda r: (r[1], r[0])))
        # Should include itself
        rdd = rdd.union(rdd.map(lambda r: (r[0], r[0])).distinct())
        # Remove dupes
        rdd = rdd.distinct()

        return rdd.groupByKey()

    def find_neighbors(self, D):
        """
        Find the neighbors of each local community
        :param D: PySpark data object
        :param lc_rdd: 
        :return: 
        """
        # Disregard weight
        graph = D.map(lambda r: (r[0], r[1]))
        # edges + edges reversed, also build connections to itself
        lc_rdd = graph.union(graph.map(lambda r: (r[1], r[0])))
        lc_rdd = lc_rdd.union(lc_rdd.map(lambda r: (r[0], r[0])))

        # nodes it's possible for second index in
        # rdd to reach in 2 edges
        neighbors_rdd = lc_rdd.join(lc_rdd).map(lambda r: (r[1][0], r[1][1]))
        neighbors_rdd = neighbors_rdd.distinct()

        # (neighbors - local community) to get rid of nodes that are in lc
        neighbors_rdd = neighbors_rdd.subtract(lc_rdd)

        return neighbors_rdd.groupByKey()

    def find_total_local_community_weight(self, D, type = "outbound"):
        """
        :param D: 
        :return: 
        """
        graph = D.map(lambda r: (r[0], r[1]))
        # edges + edges reversed, also build connections to itself
        lc_rdd = graph.union(graph.map(lambda r: (r[1], r[0])))
        lc_rdd = lc_rdd.union(lc_rdd.map(lambda r: (r[0], r[0])))
        lc_rdd = lc_rdd.distinct()

        if type == "outbound":
            D_w = D.map(lambda r: (r[0], (r[1], r[2])))
        else:
            D_w = D.map(lambda r: (r[1], (r[0], r[2])))

        # nodes it's possible for second index in
        # rdd to reach in 2 edges
        neighbors_rdd = lc_rdd.join(D_w).map(lambda r: ((r[1][0], r[1][1][0]), r[1][1][1]))

        # (neighbors - local community) to get rid of nodes that are in lc
        neighbors_rdd = neighbors_rdd.subtractByKey(lc_rdd.map(lambda r: ((r[0], r[1]), 0)))

        return neighbors_rdd.map(lambda r: (r[0][0], r[1])).reduceByKey(add)
