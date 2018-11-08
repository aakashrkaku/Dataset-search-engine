import sys
import pickle
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# start a spark session
spark = SparkSession.builder.appName("Spark SQL").config("spark.some.config.option", "some-value").getOrCreate()
from pyspark.sql import Row
sc = spark.sparkContext

# load and prep the main meta data file
op1 = spark.read.format('csv').options(header='true',inferschema='true',mode='DROPMALFORMED').load('meta_data_full.csv')
op1.createOrReplaceTempView("meta")

# get the input from the user
keyword=sys.argv[1]
print(keyword)

# query all the datasets that contain the input word
query='''SELECT meta.name as name,meta.file_path from meta where lower(name) like '%'''+keyword+'''%' or lower(tags) like '%'''+keyword+'''%' or lower(category) like '%'''+keyword+'''%' or lower(attribution) like '%'''+keyword+'''%' or lower(columns) like '%'''+keyword+'''%' or lower(description) like '%'''+keyword+'''%' '''

result=spark.sql(query)
result.show(truncate=False)


