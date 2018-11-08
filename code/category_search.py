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

# load and prepare the embedding data
loaded_embeddings=pickle.load(open('loaded_embedding','rb'))
words=pickle.load(open('words','rb'))
idx2words=pickle.load(open('idx2words','rb'))

# load the meta data
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('meta_data_full.csv')
op1.createOrReplaceTempView('metadata')

# query for the input keyword in category field
keyword = sys.argv[1]
query = '''SELECT file_path,name from metadata where lower(category) like '%'''+keyword+'''%' '''
result = spark.sql(query).dropDuplicates()
result.show(truncate=False)
