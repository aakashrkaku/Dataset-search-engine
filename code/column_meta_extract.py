#!user/bin/env python
import sys
import numpy as np
import os
import pandas as pd
from csv import reader

#create pyspark session
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

#read files
file_names  = [line.rstrip('\n') for line in open('file_names_2.txt')]
big_files = [line.rstrip('\n') for line in open('top_20_big_files.txt')]
lookup = 'meta.view.columns'
file_names_list = []
column_names_list = []
type_of_col = []
min_col = []
max_col = []
nulls = []
non_nulls = []
field_names = []
position_info = []
description_info = []

#loop through file names
for k,f in enumerate(file_names):
	print(k)
	if f in big_files:
		continue
	df_temp = spark.read.json(f,multiLine = True)
	
	#get the column metadata from the file
	try:
		col = (df_temp.select(lookup).collect())[0][0]
	except:
		col = None
		continue

	#store all the column meta datas in lists
	for c in col[8:]:
		file_names_list.append(f)
		try:
			column_names_list.append(c.name)
		except:
			column_names_list.append(None)
		try:
			type_of_col.append(c.dataTypeName)
		except:
			type_of_col.append(None)
		try:
			min_col.append(c.cachedContents.smallest)
		except:
			min_col.append(None)
		try:
			max_col.append(c.cachedContents.largest)
		except:
			max_col.append(None)
		try:
			nulls.append(c.cachedContents.null)
		except:
			nulls.append(None)
		try:
			non_nulls.append(c.cachedContents.not_null)
		except:
			non_nulls.append(None)
		try:
			field_names.append(c.fieldName)
		except:
			field_names.append(None)

		try:
			position_info.append(c.position)
		except:
			position_info.append(None)
		try:
			description_info.append(c.description)
		except:
			description_info.append(None)
columns = ['file_name', 'column_name', 'type', 'minimum', 'maximum', 'nulls', 'non_nulls', 'field_name', 'position', 'description']

values_list = [file_names_list, column_names_list, type_of_col, min_col, max_col, nulls, non_nulls, field_names, position_info, description_info]

#create dataframe of metadata of columns
df = pd.DataFrame(np.transpose(values_list),columns = columns)
df.to_csv('table_col_meta.csv', index = False)
