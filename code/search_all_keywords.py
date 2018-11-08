import sys
import pickle
import numpy as np

from similarity import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functools import reduce
from pyspark.sql import DataFrame

#build a spark session
spark = SparkSession.builder.appName("Spark SQL").config("spark.some.config.option", "some-value").getOrCreate()

#load the meta_data.csv file containing meta data of all datasets
meta = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('meta_data_full.csv')
meta.createOrReplaceTempView("meta")


#read the command line argument
kw = sys.argv[1]
all_keywords = kw.split(",")
all_df=[]

#search for keywords induvidually and store the result as a list
for keyword in all_keywords:
	query='''SELECT meta.name,meta.file_path from meta where lower(name) like '%'''+keyword+'''%' or lower(tags) like '%'''+keyword+'''%' or lower(category) like '%'''+keyword+'''%' or lower(attribution) like '%'''+keyword+'''%' or lower(columns) like '%'''+keyword+'''%' or lower(description) like '%'''+keyword+'''%'  '''
	metadf=spark.sql(query)
	all_df.append(metadf)


#Take intersection of all results and discplay the result
result = all_df[0]
for i in range(1,len(all_df)):
	result = result.intersect(all_df[i])
result.show(truncate= False)

