import pyspark
import boto3
import redis
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.session import SparkSession


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def get_id(line):
    parenthesis = "\"" # string literal for "
    paper_id_tag = "\"id\"" # find the first occurence of "id"
    id_label_start = line.find(paper_id_tag) # this is the index that the id label starts
    id_tag_start = id_label_start + 6 # this is the index that the id tag starts. Always be 6.
    id_tag_end = line.find(parenthesis, id_tag_start)  #this is the index that the id tag ends
    id_tag = line[id_tag_start:id_tag_end] # id tag string 
    return id_tag

def get_title(line):
    # look for the title of the paper and return the tag 
    paper_title_tag = "\"title\""
    parenthesis = "\"" # string literal for "
    
    title_label_start = line.find(paper_title_tag) # index for the title label start 
    title_tag_start = title_label_start + 9
    title_tag_end = line.find(parenthesis+",\"", title_tag_start) 
    title_tag = line[title_tag_start:title_tag_end]
    if title_tag[-1] == ".":
        title_tag = title_tag.replace(".", "")
    if title_tag[-1] == "]":
        title_tag = title_tag.replace("]", "")
        title_tag = title_tag.replace("[", "")
    if "\\\"" in title_tag:
        title_tag = title_tag.replace("\\\"", "\"")
    return title_tag

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
    add_ids = df.withColumn("id", get_id_udf(df.value))
    return add_ids

def adding_titles(df):
    '''
    This function takes the raw data dataframe and adds on an id column for the data
    Ex: 
    value        id 
    laeinaelk    23402939423
    lakeflake    02398402384
    ieifniena    23402938402
    '''
    add_titles = df.withColumn("title", get_title_udf(df.value))
    return add_titles

def adding_abstracts(df):
    '''
    This function takes the raw + id dataframe and adds on abstracts column for the data
    Ex
    value        id             abstracts
    laeinaelk    23402939423    Mastering the game of ...
    lakeflake    02398402384    When people go outside...
    ieifniena    23402938402    Data engineers love to...
    '''
    add_abstracts = df.withColumn("abstracts", get_abstract_udf(df.value))
    return add_abstracts

def drop_values(df):
    '''
    This function takes the dataframe and drops the value column
    Ex
    id             abstracts
    23402939423    Mastering the game of ...
    02398402384    When people go outside...
    23402938402    Data engineers love to...
    '''
    return df.drop(df.value)

# create a user defined function for get_id and get_abstract, which is compatable with a spark dataframe 
get_id_udf = udf(lambda line: get_id(line), StringType())
get_abstract_udf = udf(lambda line: get_abstract(line), StringType())
get_title_udf = udf(lambda line: get_title(line), StringType())

# read in the raw data file 
filenames = "s3a://open-research-corpus/sample-S2-records.gz" # path to the example file from S3 file 
#filenames = "s3a://open-research-corpus/corpus-2019-01-31/s2-corpus-00.gz"
df = spark.read.text(filenames)
#raw_and_ids = adding_ids(raw_data)
#raw_ids_abstracts = adding_abstracts(raw_and_ids)
#ids_abstracts = drop_values(raw_ids_abstracts)
df = adding_ids(df)
df = adding_titles(df)
df = adding_abstracts(df)
df = drop_values(df)
 
print("Schema for raw data + ids + abstracts")
print("-------------------------------------")
#raw_ids_abstracts.createOrReplaceTempView("raw_ids_and_abstracts")
#raw_ids_abstracts.printSchema()
#results = spark.sql("SELECT * FROM raw_ids_and_abstracts")
#print("First 5 entries for add_ids_abstracts data")
#ids_abstracts.createOrReplaceTempView("ids_and_abstracts")
#ids_abstracts.printSchema()
#results = spark.sql("SELECT * FROM ids_and_abstracts")
#print("Entries for ids_and_abstracts")
df.createOrReplaceTempView("filtered_df")
df.printSchema()
results = spark.sql("SELECT * FROM filtered_df")
print("Entries for filtered_df")
print("----------------------------")
results.show()
print()
print()
print("Number of rows!", results.count())
print()
print()
