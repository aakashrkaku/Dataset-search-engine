import sys
import pickle
import numpy as np
import threading
from similarity import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row

# start a spark session
conf = SparkConf().setMaster('local[*]').setAppName('appname')
conf.set('spark.scheduler.mode', 'FAIR')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# load the relevent dataset for the count of rows and columns
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('final_row_col_count.csv')
op1.createOrReplaceTempView('rowcol')

# load the relevant dataset for the main meta data
op2 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('meta_data_full.csv')
op2.createOrReplaceTempView("meta")

#read command line arguments
rowcount = sys.argv[1]
colcount = sys.argv[3]
rowupper = sys.argv[2]
colupper = sys.argv[4]

# query for the datasets that meet the conditions of the input row and column numbers
query = '''SELECT rowcol.file_names,meta.name from rowcol right join meta on rowcol.file_names=meta.file_path where num_rows >'''+str(rowcount)+''' and num_cols >'''+str(colcount)+''' and num_rows<'''+str(rowupper)+''' and num_cols<'''+str(colupper)+''' '''
result = spark.sql(query).dropDuplicates()
result.show(truncate=False)
