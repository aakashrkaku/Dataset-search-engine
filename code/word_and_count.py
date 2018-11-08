import sys
import pickle
import numpy as np
import threading

from similarity import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row

# start a spark sesion
conf = SparkConf().setMaster('local[*]').setAppName('appname')
conf.set('spark.scheduler.mode', 'FAIR')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# load the embedding data
loaded_embeddings=pickle.load(open('loaded_embedding','rb'))
words=pickle.load(open('words','rb'))
idx2words=pickle.load(open('idx2words','rb'))

# load the preprocessed word count dataset
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('final_wc_file.csv')
op1.createOrReplaceTempView("wordcount")

#op1.printSchema()

# load the main meta data file
op2 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('meta_data_full.csv')
op2.createOrReplaceTempView("meta")

# get the input word and the desired count from the user
keyword = sys.argv[1]
c = sys.argv[2]

all_words = []
res = []
threads = []

def task(sc,query):
	resul = spark.sql(query)
	resul.show()
	res.append(resul)


# query for the datasets which contain the input word and
query = '''SELECT wordcount.file_name,meta.name from wordcount right join meta on wordcount.file_name=meta.file_path where lower(word) like '%'''+keyword+'''%' and value >'''+str(c)+''' '''

result = spark.sql(query).dropDuplicates()
result.show(truncate=False)

