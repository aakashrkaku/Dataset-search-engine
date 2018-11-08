import sys
import pickle
import numpy as np

from similarity import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#build a spark session
spark = SparkSession.builder.appName("Spark SQL").config("spark.some.config.option", "some-value").getOrCreate()
from pyspark.sql import Row
sc = spark.sparkContext

#load the meta_data.csv file containing meta data of all datasets
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('Cached_Data/meta_data.csv')
op1.createOrReplaceTempView("meta")

#key word to search throught the metadata
keyword='public'

#build a quesry to search for the specified keyword
query='''SELECT meta.name as name,meta.attributionLink as link from meta where lower(name) like '%'''+keyword+'''%' or lower(tags) like '%'''+keyword+'''%' or lower(category) like '%'''+keyword+'''%' or lower(attribution) like '%'''+keyword+'''%' or lower(columns) like '%'''+keyword+'''%' or lower(description) like '%'''+keyword+'''%' '''

#Use pyspark's sql to perform the querying through table
result=spark.sql(query)

#show the result to user
result.show()