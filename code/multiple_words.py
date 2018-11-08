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

# load the embedding data
loaded_embeddings=pickle.load(open('loaded_embedding','rb'))
words=pickle.load(open('words','rb'))
idx2words=pickle.load(open('idx2words','rb'))

# load the pre-processde word count data file
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('final_wc_file.csv')
op1.createOrReplaceTempView("wordcount")

# load the main meta data file
op2 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('meta_data_full.csv')
op2.createOrReplaceTempView("meta")


# get the input keywords and process them
keywords = sys.argv[1]
k = keywords.split(',')
print(k)
all_words = []
res = []
threads = []

# find the 3 most similar words for each of the input keywords
for i in k:
	try:
		similar_words=find_nearest(loaded_embeddings[words[i]], words, loaded_embeddings,topk=3)[1:]
	except:
		similar_words = []
	all_words = all_words + similar_words
all_words = all_words + k
print(all_words)

def task(sc,query):
	resul = spark.sql(query)
	res.append(resul)

# for each word (including similar words) query the datasets that contain the work using threading method to speed up the search
for keyword in all_words:
	query = '''SELECT wordcount.file_name,meta.name from wordcount right join meta on  wordcount.file_name=meta.file_path where lower(word) like '%'''+keyword+'''%' '''
	t = threading.Thread(target=task, args=(sc,query))
	t.start()
	threads.append(t)

for x in threads:
        x.join()

final_df = res[0]

# show any datasets that contain at least one of the input words or its similar words
for i in range(1,len(res)):
	final_df = final_df.union(res[i])
#drop duplicates
final_df = final_df.dropDuplicates()
final_df.show(truncate=False)
