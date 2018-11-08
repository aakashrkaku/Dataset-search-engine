import sys
import pickle
import numpy as np
import threading
import numpy as np
import os
import pandas as pd
from csv import reader

import json
import time

from similarity import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row

#create pyspark session
conf = SparkConf().setMaster('local[*]').setAppName('appname')
conf.set('spark.scheduler.mode', 'FAIR')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
file_n = sys.argv[1]


df = spark.read.json(file_n,multiLine=True)
try:
	data_col = df.select('data').collect()
except:
	x = file_n.split('/')[-1]

	f = open('/scratch/ark576/big_data/'+str(x),'r')
	data = json.load(f)
	data = data['data']
 
print("continues")
