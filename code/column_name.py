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

# load the meta data
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('final_wc_file.csv')
op1.createOrReplaceTempView('metadata')

op2 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('meta_data_full.csv')
op2.createOrReplaceTempView("meta")

col_name = sys.argv[1]
#col_val = sys.argv[2]

# query for the input keyword in category field
keyword = sys.argv[1]
query = '''SELECT metadata.file_name as path,meta.name from metadata right join meta on metadata.file_name=meta.file_path where lower(col_name) like '%'''+col_name+'''%' '''
result = spark.sql(query).dropDuplicates()
result.show(truncate=False)
