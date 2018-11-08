import sys
import os
print("\n")
print("#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*")
print("#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*")
print("\n")
print("\t\t\t\t\t**** WELCOME ****")
print("\n")
print("If you are looking for general datasets, take a look at our famous datasets")

print("**********************************************************************************************")
print("5 most viewed datasets:")
print("------------------------")
# show the most popular datasets by running the corresponding file
f = open("query_most_viewed.out",'r')
mostviewed = f.read()
print(mostviewed)
print("\t\t\t###############################################")

# show the most downloaded datasets by running the corresponding file
f = open("query_most_download.out",'r')
print("5 most downloaded datasets:")
print("---------------------------")
mostdownloaded = f.read()
print(mostdownloaded)
print("**********************************************************************************************")
print("\n")
print("Else,")
# let the user choose among the possible types of queries
while(1):
	print("Choose one of the query types")
	print("\n")
	print("1 Search for  keyword")
	print("2 Search for a keyword with threshold on number of times it should occur")
	print("3 Search for a keyword with threshold on number of rows the datasets should contain")
	print("4 Dataset belonging to certain category")
	print("5 Dataset with certain number of rows and columns")
	print("6 See the Schema of Dataset")
	print("7 Search for multiple keywords in OR fashion")
	print("8 Searcg for multiple keywords in AND fashion")
	print("9 Search for files with certain columns in it")
	print("10 Search for files with certain column and value")
	print("11 Search keywords in meta data and data")

	print("\n")
	# get the input from the user
	choice = int(input("Enter Your Choice \n"))
	# run the relevant script to the user's choice. Let the user insert relevant inputs such as keywords, etc
	if choice == 1:
		choice2 = input("Enter the keyword to search \n")
		argument = "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/search.py "+choice2
		print(argument)
		os.system(argument)

	elif choice == 2:
		choice2 = input("Enter the keyword to search for \n")
		choice3 = int(input("ENter the thhreshold on keyword\n"))
		argument = "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/word_and_count.py "+choice2+" "+str(choice3)
		print(argument)
		os.system(argument)

	elif choice == 3:
		choice2 = input("Enter the keyword to search for\n")
		choice3 = input("Enter the threshold on number of rows in Dataset\n")
		argument = "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/word_and_num_col.py "+choice2+" "+str(choice3)
		os.system(argument)

	elif choice == 4:
		choice2 = input("Enter the category to search for\n")
		argument = "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/category_search.py "+choice2
		os.system(argument)

	elif choice == 5:
		choice2 = input("Enter minimum number of rows\n")
		choice3 = input("Enter maximum number of rows\n")
		choice4 = input("Enter minimum number of columns\n")
		choice5 = input("Enter maximum number of columns\n")
		argument = "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/min_max_row_col.py "+choice2+" "+choice3+" "+choice4+" "+choice5
		os.system(argument)

	elif choice == 6:
		choice2 = input("Enter file path to view the schema\n")
		argument = "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/complex.py "+choice2
		os.system(argument)

	elif choice == 7:
		choice2  = input("input the words with commma seperating them\n")
		argument =  "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/multiple_words.py "+choice2
		os.system(argument)

	elif choice == 8:
		choice2 = input("input the words with comma seperating them\n")
		argument =  "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/search_all_keywords.py "+choice2
		os.system(argument)

	elif choice == 9:
		choice2 = input("Enter the column name\n")
		argument = "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/column_name.py "+choice2
		os.system(argument)

	elif choice == 10:
		choice2 = input("Enter the column name\n")
		choice3 = input("Enter the keyword to look for within the column\n")
		argument = "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/col_val_type.py "+choice2+" "+choice3
		os.system(argument)
	elif choice ==11:
		choice2 = input("Enter the keyword to look for\n")
		choice3 = input("1 if you want to use similar word search or 1\n")
		argument = "spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python /home/cvh255/project/search_everywhere.py "+choice2+" "+choice3
		os.system(argument)
	print("**********************************************************************************************")
