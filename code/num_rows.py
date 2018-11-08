#!user/bin/env python
import sys
import numpy as np
import os
import pandas as pd
from csv import reader

import json
import time

#read filenames
file_names  = [line.rstrip('\n') for line in open('/scratch/ark576/scratch_file_names.txt')]
big_files = [line.rstrip('\n') for line in open('/scratch/ark576/top_20_big_files.txt')]
fnames = []
num_rows = []
num_cols = []

#function to read files and save number of rows and columns in the file
def task(flist):
	for z,f in enumerate(flist):
		hfs_path = '/user/bigdata/nyc_open_data/'+f
		if hfs_path in big_files:
			continue
		try:
			file = open('/scratch/ark576/big_data/'+f,'r')
			data = json.load(file)
			data = data['data']
			df = pd.DataFrame(data)
			df = df.iloc[:,8:]
			fnames.append(hfs_path)
			num_rows.append(df.shape[0])
			num_cols.append(df.shape[1])
		except:
			continue

task(file_names)
values_list = [fnames,num_rows,num_cols]
columns = ["file_names", "num_rows", "num_cols"]

df = pd.DataFrame(np.transpose(values_list),columns = columns)

df.to_csv("row_col_count.csv")
