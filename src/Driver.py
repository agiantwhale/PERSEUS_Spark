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

from Configurations import Configurations

from Degrees import Degrees
from PageRank import PageRank
    
# 
# parse the raw data, rendering it to start with index 0
def parse_raw_lines(line):
    if line.startswith('#'):
        return -1, -1
    else:
        line = line.strip('\r\n')
        parts = re.split('\t| ', line)
        x_id = int(parts[0])
        y_id = int(parts[1])
        return x_id, y_id

def flatmap_add_index(line):
    row_index = line[1]
    row = line[0]
    for col_index in range(len(row)):
        yield col_index, (row_index, row[col_index])

def map_make_col(line):
    col_index = line[0]
    col = line[1]
    res = []
    for item in sorted(col):
        print (item)
        res.append(item[1])
    return col_index, np.array(res)



#########################################################################################

if __name__ == '__main__':
    
    if len(sys.argv) != 3:
        print ('Usage: spark-submit Degrees.py <input_filepath> <output_filepath>')
        sys.exit(-1)

    data_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    
    # default settings
    initialID = 1

    # setting up spark
    os.environ["SPARK_HOME"] = "/Users/DiJin/BigData/spark-1.6.0-bin-hadoop2.6"    # <<<---
    # configure the Spark environment
    sparkConf = pyspark.SparkConf().setAppName("PERSEUS_Spark")\
    .setMaster("local")                                                            # <<<---
    sc = pyspark.SparkContext(conf = sparkConf)
    
    # load data file with format:
    # <src>    <dst>
    # remove all self loops and titles
    lines = sc.textFile(data_file_path)
    tempD = lines.map(lambda line: parse_raw_lines(line)).cache()
    D = tempD.filter(lambda x: x[0] > 0 and x[0] != x[1]).cache()
    
    ut.printRDD(D)
#     find the max value for user_id and movie_id
    
    graph_statistics = Configurations()
    
    '''
    Degrees
    '''
    deg = Degrees()
    
    if graph_statistics.getIndeg():
        output_rdd = deg.statistics_compute(D, 'out')
        
        # generate outputs to hdfs
        temp = output_rdd.map(lambda x: "\t".join(map(str,x))).coalesce(1)
        temp.saveAsTextFile(output_file_path+'out_degree')
        
    if graph_statistics.getOutdeg():
        output_rdd = deg.statistics_compute(D, 'in')
        
        # generate outputs to hdfs
        temp = output_rdd.map(lambda x: "\t".join(map(str,x))).coalesce(1)
        temp.saveAsTextFile(output_file_path+'in_degree')
        
    if graph_statistics.getTotaldge():
        output_rdd = deg.statistics_compute(D, 'total')
        
        # generate outputs to hdfs
        temp = output_rdd.map(lambda x: "\t".join(map(str,x))).coalesce(1)
        temp.saveAsTextFile(output_file_path+'total_degree')
        
    '''
    PageRank
    '''
    pr = PageRank()
    
    if graph_statistics.getIndeg():
        output_rdd = pr.statistics_compute(D, 'out')
        
        # generate outputs to hdfs
        temp = output_rdd.map(lambda x: "\t".join(map(str,x))).coalesce(1)
        temp.saveAsTextFile(output_file_path+'out_degree')
   
#          
    
#      
#     Edges_rdd = sc.parallelize(Edges)
#     temp = Edges_rdd.map(lambda x: ",".join(map(str,x))).coalesce(1)
#     temp.saveAsTextFile(output_file_path+'edges')
