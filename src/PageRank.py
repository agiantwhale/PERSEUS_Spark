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

class PageRank:
    
    def __init__(self):
        self._descriotion = 'In pagerank'
        
    def statistics_compute(self, D):
        
        