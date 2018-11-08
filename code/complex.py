import sys
import pickle
import numpy as np
import threading

from similarity import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
#create pyspark session
conf = SparkConf().setMaster('local[*]').setAppName('appname')
conf.set('spark.scheduler.mode', 'FAIR')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

#read the command line argument
file_n = sys.argv[1]

#read the meta data.csv file 
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('table_col_meta.csv')
op1.createOrReplaceTempView("meta")

#query the column name and its type
query = '''select field_name,type from meta where file_name like '%'''+file_n+'''%' '''
res = spark.sql(query)
res.show(truncate=False)
