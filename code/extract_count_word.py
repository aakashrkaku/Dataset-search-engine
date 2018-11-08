#!user/bin/env python
import sys
import numpy as np
import os
import pandas as pd
from csv import reader
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import re
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
import json
import time
from multiprocessing import Pool
num_partitions = 20 #number of partitions to split dataframe
num_cores = 10 #number of cores on your machine
def parallelize_dataframe(df, func):
	df_split = np.array_split(df, num_partitions)
	pool = Pool(num_cores)
	df = pd.concat(pool.map(func, df_split))
	pool.close()
	pool.join()
	return df
def tokenize(data):
	data = data.str.strip().str.split('[\W_]+')
	return data
def filternull(data):
    data = data[data.notnull()]
    return data
from nltk.corpus import stopwords
start_time = time.time()

file_names  = [line.rstrip('\n') for line in open('/scratch/ark576/scratch_file_names.txt')]
big_files = [line.rstrip('\n') for line in open('/scratch/ark576/top_20_big_files.txt')]
col_meta_data = pd.read_csv('/scratch/ark576/table_col_meta_2.csv')
stop_words_list = set([line.rstrip('\n') for line in open('/scratch/ark576/stopwords.txt')])

for z,f in enumerate(reversed(file_names[1500:1858])):
	hfs_path = '/user/bigdata/nyc_open_data/'+f
	if hfs_path in big_files:
		continue
	try:
		file = open('/scratch/ark576/big_data/'+f,'r')
		data = json.load(file)
		data = data['data']
		df = pd.DataFrame(data)
		df = df.iloc[:,8:]
	except:
		continue
	rel_cols_df = col_meta_data[col_meta_data['file_name'] == hfs_path][['column_name','type_2']]
	if len(rel_cols_df) == 0:
		continue
	df_list = []
	for i,col in enumerate(df.columns):
		try:
			temp_seri = df[col]
			temp_seri = temp_seri[temp_seri.notnull()]
		except:
			continue
		try:
			if rel_cols_df.iloc[i].type_2 not in ['text','url','html']:
				continue
		except:
			continue
		try:
			temp_seri = temp_seri.str.strip().str.split('[\W_]+')
		except:
			continue
		rows = list()
		for row in temp_seri:
			try:
				for word in row:
					if len(word)>3:
						word = word.lower()
						if word not in set(stop_words_list):
							rows.append(word)
			except:
				continue
		words = pd.Series(rows)
		temp_count = words.value_counts()[:200]
		temp_df = pd.DataFrame()
		temp_df['word'] = temp_count.index
		temp_df['value'] = temp_count.values
		temp_df['file_name'] = hfs_path
		temp_df['col_name'] = rel_cols_df.iloc[i].column_name
		df_list.append(temp_df)
	if len(df_list)>0:
		df_final = pd.concat(df_list,axis=0)
		saved_file_name = '/scratch/ark576/processed_wc_files_2/'+f+'_processed.csv'
		df_final.to_csv(saved_file_name, index = False)
		print(z)

end_time = time.time()
print("time taken = ",(end_time-start_time))