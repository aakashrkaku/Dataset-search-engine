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

# load and prep the main metadata file
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('meta_data_full.csv')
op1.createOrReplaceTempView("meta")

# query for the top 5 most popular and most downloaded files
res_viewed = spark.sql('''select meta.name,meta.attributionLink as link,meta.file_path from meta order by meta.viewCount DESC limit 5''')
res_download = spark.sql('''select meta.name,meta.attributionLink as link,meta.file_path from meta order by meta.downloadCount DESC limit 5''')

# show and save the results for 5 most viewed and downloaded datasets
print("5 most viewed datasets")
res_viewed.select(format_string('Dataset Name: %s,Link to Dataset: %s',res_viewed.name,res_viewed.link)).write.save("query_most_viewed.out",format="text")
res_viewed.show(truncate=False)

print("5 most downloaded datasets")
res_download.show(truncate=False)
res_download.select(format_string('Dataset Name: %s,Link to Dataset: %s',res_download.name,res_download.link)).write.save("query_most_download.out",format="text")
