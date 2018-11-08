# Dataset-Search-Engine

- sophisticated search engine to search for dataset for NYC Open Data containing approximately 1500 datasets (~650 GB)
- Developed 11 advanced search functionalities, currently are not supported by NYC Open Data website, to help the user find datasets
- Used PySpark to parallelize, hence speed up the execution of code. Created data summaries to quickly speed up the search process

## 11 Queries Implemeted

1) Search for  keyword
2) Search for a keyword with threshold on number of times it should occur
3) Search for a keyword with threshold on number of rows the datasets should contain
4) Dataset belonging to certain category
5) Dataset with certain number of rows and columns
6) See the Schema of Dataset
7) Search for multiple keywords in OR fashion
8) Searcg for multiple keywords in AND fashion
9) Search for files with certain columns in it
10) Search for files with certain column and value
11) Search keywords in meta data and data

## Friendly and Interactive User Interface
![Alt text](ss1.png?raw=true "Title")

## Query Example

![Alt text](s1.png?raw=true "Title")

## Query Example

![Alt text](s2.png?raw=true "Title")

## Query Example : NYC Open Data website returns nothing for keyword = "uber" but our engine does - most importantly, its a relevent one.

![Alt text](s3.png?raw=true "Title")


