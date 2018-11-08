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
flag = sys.argv[2]

#k = keywords.split(',')
#print(k)
all_words = []
res = []
threads = []

# find the 3 most similar words for each of the input keywords
if int(flag) == 1:
	try:
		similar_words=find_nearest(loaded_embeddings[words[keywords]], words, loaded_embeddings,topk=3)[1:]
	except:
		similar_words = []
	all_words = all_words + similar_words
else:
	all_worrds = []
all_words = all_words + [keywords]

res2= []
print(all_words)

def task(sc,query,query2):
	resul = spark.sql(query)
	result2 = spark.sql(query2)
	res.append(resul)
	res2.append(result2)

# for each word (including similar words) query the datasets that contain the work using threading method to speed up the search
for keyword in all_words:
	query = '''SELECT wordcount.file_name as file_path,meta.name as name from wordcount right join meta on wordcount.file_name=meta.file_path where lower(word) like '%'''+keyword+'''%' '''
	query2 = '''SELECT meta.name as name,meta.file_path as file_path from meta where lower(name) like '%'''+keyword+'''%' or lower(tags) like '%'''+keyword+'''%' or lower(category) like '%'''+keyword+'''%' or lower(attribution) like '%'''+keyword+'''%' or lower(columns) like '%'''+keyword+'''%' or lower(description) like '%'''+keyword+'''%' '''
	t = threading.Thread(target=task, args=(sc,query,query2))
	t.start()
	threads.append(t)

for x in threads:
        x.join()

final_df = res[0]
final_df2 = res2[0]
# show any datasets that contain at least one of the input words or its similar words
for i in range(1,len(res)):
	final_df = final_df.union(res[i])
	final_df2 = final_df2.union(res2[i])

final_df = final_df.dropDuplicates()
final_df2 = final_df2.dropDuplicates()
#drop duplicates
final_df = final_df.union(final_df2)
final_df = final_df.dropDuplicates()
final_df.show(truncate=False)
