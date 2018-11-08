import sys
import pickle
import numpy as np
import threading
loaded_embeddings=pickle.load(open('loaded_embedding','rb'))
words=pickle.load(open('words','rb'))
idx2words=pickle.load(open('idx2words','rb'))
from similarity import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
conf = SparkConf().setMaster('local[*]').setAppName('appname')
conf.set('spark.scheduler.mode', 'FAIR')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('meta_data_full.csv')
op1.createOrReplaceTempView("meta")

keyword='taxi'
similar_words=find_nearest(loaded_embeddings[words[keyword]], words, loaded_embeddings,topk=4)[1:]

similar_words.append(keyword)

print(similar_words)

res= []
threads=[]
def task(sc,query):
	resul = spark.sql(query)
	res.append(resul)	

for i,keyword in enumerate(similar_words):
	query='''SELECT meta.name as name,meta.attributionLink as link from meta where lower(name) like '%'''+keyword+'''%' or lower(tags) like '%'''+keyword+'''%' or lower(category) like '%'''+keyword+'''%' or lower(attribution) like '%'''+keyword+'''%' or lower(columns) like '%'''+keyword+'''%' or lower(description) like '%'''+keyword+'''%' '''
	t = threading.Thread(target=task, args=(sc, query))
	t.start()
	threads.append(t)

for x in threads:
	x.join()
trying = res[0].union(res[1]).union(res[2]).union(res[3]).dropDuplicates()
trying.show()
