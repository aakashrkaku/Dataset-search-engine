#!user/bin/env python
import sys
import numpy as np
import os
import pandas as pd
from csv import reader
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
#Import all the required packages


#build a spark session
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

#file_name.txt contain the names of all non-zero sized datasets
file_names  = [line.rstrip('\n') for line in open('Cached_Data/file_name.txt')]

#meta data to extract 
info_list = ['meta.view.attributionLink','meta.view.category','meta.view.attribution', 'meta.view.columns', 'meta.view.description','meta.view.id','meta.view.downloadCount','meta.view.name','meta.view.viewCount','meta.view.tags']

#lists to hold individual metadata
list_f = []
list_atl = []
list_cat = []
list_at = []
list_col = []
list_desc = []
list_id = []
list_downc = []
list_name = []
list_viewc = []
list_tags = []
list_all = [list_atl,list_cat,list_at,list_col,list_desc, list_id, list_downc, list_name,list_viewc, list_tags]

#looping through file names
for k,f in enumerate(file_names):
	#load the json file 
	df_temp = spark.read.json(f,multiLine = True)
	
	#loop through meta data to extract only required fields
	for i,j in enumerate(info_list):
		try:
			val = (df_temp.select(j).collect())[0][0]
		except:
			val = None
		list_all[i].append(val)
	list_f.append(f)
list_all.append(list_f)
columns = ['attributionLink','category','attribution','columns','description','id','downloadCount','name','viewCount','tags','file_path']

#create a dataframe of metadata of all datasets
df = pd.DataFrame(np.transpose(list_all),columns = columns)

#Cache the dataframe to CSV file for future use
df.to_csv('meta_data.csv',index = False)