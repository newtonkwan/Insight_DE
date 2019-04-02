'''
Newton Kwan
Created: April 2, 2019 
Velma: Insight Data Engineering 

This file preprocesses text files and stores them back into AWS S3 
'''
import redis
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

 
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def get_id(line):
    '''
    This function extracts the ID from a text line from the dataframe 
    '''
    parenthesis = "\"" # string literal for "
    paper_id_tag = "\"id\"" # find the first occurence of "id"
    id_label_start = line.find(paper_id_tag) # this is the index that the id label starts
    id_tag_start = id_label_start + 6 # this is the index that the id tag starts. Always be 6.
    id_tag_end = line.find(parenthesis, id_tag_start)  #this is the index that the id tag ends
    id_tag = line[id_tag_start:id_tag_end] # id tag string 
    return id_tag

def get_abstract(line):
    parenthesis = "\"" # string literal for "
    paper_abstract_tag = "\"paperAbstract\""
    abstract_label_start = line.find(paper_abstract_tag) # index that the abstrat label starts 
    abstract_tag_start = abstract_label_start + 17 # the start of the abstract tag 
    abstract_tag_end = line.find(parenthesis, abstract_tag_start) # the end of the abstract tag
    abstract_tag = line[abstract_tag_start:abstract_tag_end] # abstract tag string 
    return abstract_tag

def adding_ids(df):
    '''
    This function takes the raw data dataframe and adds on an id column for the data
    Ex: 
    value        id 
    laeinaelk    23402939423
    lakeflake    02398402384
    ieifniena    23402938402
    '''
    add_ids = df.withColumn("id", get_id_udf(raw_data.value))
    return add_ids

def adding_abstracts(df):
    '''
    This function takes the raw + id dataframe and adds on abstracts column for the data
    Ex
    value        id             abstracts
    laeinaelk    23402939423    Mastering the game of ...
    lakeflake    02398402384    When people go outside...
    ieifniena    23402938402    Data engineers love to...
    '''
    add_ids_abstracts = df.withColumn("abstracts", get_abstract_udf(raw_data.value))
    return add_ids_abstracts

# create a user defined function for get_id and get_abstract, which is compatable with a spark dataframe 
get_id_udf = udf(lambda line: get_id(line), StringType())
get_abstract_udf = udf(lambda line: get_abstract(line), StringType())

# read in one of the very small (1MB) raw data sample files 
#filenames = "s3a://open-research-corpus/sample-S2-records.gz" # path to the example file from S3 file 

# this is one of the raw data files (1GB) 
filenames = "s3a://open-research-corpus/corpus-2019-01-31/s2-corpus-00.gz"

raw_data = spark.read.text(filenames)
raw_and_ids = adding_ids(raw_data)
raw_ids_abstracts = adding_abstracts(raw_and_ids)

print("Schema for raw data + ids + abstracts")
print("-------------------------------------")
raw_ids_abstracts.createOrReplaceTempView("raw_ids_and_abstracts")
raw_ids_abstracts.printSchema()
results = spark.sql("SELECT * FROM raw_ids_and_abstracts")
print("First 5 entries for add_ids_abstracts data")
print("----------------------------")
results.show()
print()
print()
print("Number of rows!", results.count())
print()
print() 
