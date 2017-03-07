'''
Created on Feb 26, 2017

@author: DiJin
'''
import sys
import os
import re
import random
import numpy as np
import pyspark
from operator import add
from pyspark import SparkConf, SparkContext

import Utility as ut

class PageRank:
    
    def __init__(self):
        self._descriotion = 'In pagerank'

        
    def computeContribs(self, urls, rank):
        """Calculates URL contributions to the rank of other URLs."""
        
        if urls is not None:
            num_urls = len(urls)
            for url in urls:
                yield (url, rank / num_urls)
        
    def statistics_compute(self, D, Iter, d):
        max_src, max_dst = D.reduce(lambda x, y: (max(x[0], y[0]), max(x[1], y[1])))
        print(max_src, max_dst)
        
        N = float(max(max_src, max_dst))
        
        ranks = D.map(lambda line: line[0]).union(D.map(lambda line: line[1])).distinct().map(lambda line: (line, 1/N)).cache()
        ut.printRDD(ranks)
        for iter in xrange(Iter):
            print(iter)
            contribs = D.groupByKey().rightOuterJoin(ranks).flatMap(
            lambda url_urls_rank: self.computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
            )
#             ut.printRDD(contribs)
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * d + (1-d)/N)
#             ut.printRDD(ranks)
        return ranks
#         for (link, rank) in ranks.collect():
#             print("%s has rank: %s." % (link, rank))
        
        