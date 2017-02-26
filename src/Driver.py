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

import Degrees as deg
    
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



# update through SGD
def egoProperties_by_partition(iterator, broadcastD, broadcastTotal):
    node_set = set()
    node = 0
#     H_set = set()

    for ele in iterator:
        node = ele[0]
        (src, dst) = ele[1]
        node_set.add(src)
        node_set.add(dst)
        
        
    results = []
    
    temp = []
    for i in node_set:
        temp.append(i)
        
    
    srcs = [0] * broadcastTotal.value
    dsts = [0] * broadcastTotal.value
    all = [0] * broadcastTotal.value
#     
    for index, item in enumerate(broadcastD.value):

        if item[0] in temp:
            srcs[index] = 1
        if item[1] in temp:
            dsts[index] = 1
#     print srcs
#     print '-----------------------------'
#     print dsts

    for i in xrange(broadcastTotal.value):
        all[i] = srcs[i] + dsts[i]
    
    edgeCount = all.count(2)
        
    
    results.append(('ego_edge', node, edgeCount))  
    results.append(('ego_size', node, len(node_set)))

 
    return results

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
    # <user_id>,<movie_id>,<rating>
    lines = sc.textFile(data_file_path)
    tempD = lines.map(lambda line: parse_raw_lines(line)).cache()
    D = tempD.filter(lambda x: x[0] > 0).cache()
    
    ut.printRDD(D)
#     find the max value for user_id and movie_id
    max_x_id, max_y_id = D.reduce(lambda x, y: (max(x[0], y[0]), max(x[1], y[1])))
    print(max_x_id, max_y_id)
    
    N = max(max_x_id, max_y_id)+1
    
    # initialize the outputs
    Sizes = np.random.randint(2, size=(N-1, 1))
    Edges = np.random.randint(2, size=(N-1, 1))
    partition_list = []
    
    for i in xrange(initialID, N):
#
        partition = D.filter(lambda x: i in x).map(lambda x: (i, x)).cache()
#         printRDD(partition)    
        partition_list.append(partition)
#         print i
    
    broadcastD = sc.broadcast(D.collect())
    broadcastTotal = sc.broadcast(D.count())
    
    counter = 0
    # iterate through all nodes for per-node local property
    for partition in partition_list:
        
        counter = counter + 1
        print(counter)
        results = partition.mapPartitions(lambda x: egoProperties_by_partition(x, broadcastD, broadcastTotal)).collect()       

        # find all properties 
        for ele in results:
            if ele[0] == 'ego_size':
#                 print ele[1], ele[2]
                Sizes[ele[1]-initialID] = ele[2]
            elif ele[0] == 'ego_edge':
                Edges[ele[1]-initialID] = ele[2]
                

                     
#     print Sizes
#     print '======================='
#     print Edges
   
#          
    # generate sizes to hdfs
    Sizes_rdd = sc.parallelize(Sizes)
#     printRDD(W_rdd)
    temp = Sizes_rdd.map(lambda x: ",".join(map(str,x))).coalesce(1)
    temp.saveAsTextFile(output_file_path+'sizes')
#      
    Edges_rdd = sc.parallelize(Edges)
    temp = Edges_rdd.map(lambda x: ",".join(map(str,x))).coalesce(1)
    temp.saveAsTextFile(output_file_path+'edges')
