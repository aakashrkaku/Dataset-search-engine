import sys
import pickle
import numpy as np
import threading

from similarity import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row

#create spark session
conf = SparkConf().setMaster('local[*]').setAppName('appname')
conf.set('spark.scheduler.mode', 'FAIR')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

#read all the required files into pyspark
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('final_wc_file.csv')
op1.createOrReplaceTempView("wordcount")
op2 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load("final_row_col_count.csv")
op2.createOrReplaceTempView("rowcolcount")
op3 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('meta_data_full.csv')
op3.createOrReplaceTempView("meta")

#read command line arguments
keyword = sys.argv[1]
c = int(sys.argv[2])

#query for dataset with keyword and partaining to certain threshold on number of columns
query = '''SELECT wordcount.file_name,meta.name from wordcount LEFT JOIN rowcolcount on wordcount.file_name = rowcolcount.file_names right join meta on wordcount.file_name= meta.file_path  WHERE lower(word) like '%'''+keyword+'''%' and num_rows >'''+str(c)+''' '''
result = spark.sql(query).dropDuplicates()
result.show(truncate=False)
