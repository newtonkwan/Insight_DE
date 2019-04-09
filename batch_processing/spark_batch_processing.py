'''
Newton Kwan
April 5, 2019 
Velma 

This file computes the jaccard index from a dataframe 
'''
import redis
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

# set up necessary environment variables 
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# name of bucket 
preprocessed_bucket_name = "preprocessed-open-research-corpus"

# read in the dataframe from S3 
def read_parquet_from_bucket(bucket_name):
    '''
    Read a parquet from a AWS bucket
    '''
    return spark.read.parquet("s3a://{0}/*.parquet".format(bucket_name))

df = read_parquet_from_bucket(preprocessed_bucket_name)

# loop over the values of the dataframe 

def compute_jaccard_index(df):
	return 

def store_to_redis(line):
    '''
    Take a line from the dataframe and store it into redis
    This doesn't need to return anything, but spark requires it 
    '''
    rdb = redis.Redis(host="10.0.0.5", port="6379")
    #rdb.set(line['id'], line['abstracts'])
    rdb.lpush(line['id'],line['title'])
    rdb.lpush(line['id'],line['abstracts'])
    return line

q = df.rdd.map(store_to_redis) # dummy variable name to store to redis 
q.count() # activation function

print("Schema for filtered data")
print("-------------------------------------")
df.createOrReplaceTempView("filtered_df")
df.printSchema()
results = spark.sql("SELECT * FROM filtered_df")
print("Entries for filtered_df")
print("----------------------------")
results.show()
print()
print()
print("Number of rows!", results.count())


