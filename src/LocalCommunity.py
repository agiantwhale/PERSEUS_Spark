'''
Created on April 9, 2017

@author: Il Jae Lee (iljae@umich.edu)
'''


class LocalCommunity:

    def __init__(self):
        self._descriotion = 'In local community'

    def find_local_community(self, D):
        """
        Finds the local community of each node
        :param D: PySpark data object
        :return: PySpark RDD object
        """
        return D.union(D.map(lambda r: (r[1], r[0]))).groupByKey()

    def find_neighbors(self, D, lc_rdd = None):
        """
        Finds the neighbors of each local community
        :param D: PySpark data object
        :param lc_rdd: Return value of find_local_community() -- if null, 
                        new one will be calculated.
        :return: PySpark RDD object
        """
        if lc_rdd == None:
            lc_rdd = self.find_local_community(D)

        return None
