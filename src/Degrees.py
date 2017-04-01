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
from pyspark import SparkConf, SparkContext

import Utility as ut
    
class Degrees:
    
    def __init__(self):
        self._descriotion = 'In degrees'
        
    def statistics_compute(self, D, mod):
        
        if mod == 'out':
            output_rdd = D.groupByKey().map(lambda r: (r[0], len(r[1])))
            print("out degree completed")
            return output_rdd
        
        if mod == 'in':
            output_rdd = D.map(lambda r: (r[1], r[0])).groupByKey().map(lambda r: (r[0], len(r[1])))
            print("in degree completed")
            return output_rdd
        
        if mod == 'total':
            output_rdd = D.union(D.map(lambda r: (r[1], r[0]))).groupByKey().map(lambda r: (r[0], len(r[1])))
            print("total degree completed")
            return output_rdd
        
        if mod == 'weighted_out':
            output_rdd = D.map(lambda x: (x[0], x[2])).groupByKey().map(lambda r: (r[0], sum(r[1])))
            print("out weighted degree completed")
            return output_rdd

        if mod == 'weighted_in':
            output_rdd = D.map(lambda x: (x[1], x[2])).groupByKey().map(lambda r: (r[0], sum(r[1])))
            print("in weighted degree completed")
            return output_rdd
            
        if mod == 'weighted_total':
            output_rdd = D.map(lambda x: (x[0], x[2])).union(D.map(lambda x: (x[1], x[2]))).groupByKey().map(lambda r: (r[0], sum(r[1])))
            print("total weighted degree completed")
#             ut.printRDD(output_rdd)
            return output_rdd
