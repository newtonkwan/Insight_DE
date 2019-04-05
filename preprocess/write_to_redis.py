'''
Newton Kwan
April 4, 2019 
Velma

This file reads a parquet file from an AWS bucket
and stores the results to a redis database
'''

import redis
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import udf

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def read_parquet_from_bucket(bucket_name):
    '''
    Read a parquet from a AWS bucket
    '''
    return spark.read.parquet("s3a://{0}/*.parquet".format(bucket_name))

df = read_parquet_from_bucket(preprocessed_bucket_name)

def store_to_redis(line):
    '''
    Take a line from the dataframe and store it into redis
    This doesn't need to return anything, but spark requires it 
    '''
    rdb = redis.Redis(host="10.0.0.5", port="6379")
    rdb.set(line['id'], line['abstracts'])
    return line

q = df.rdd.map(store_to_redis) # dummy variable name to store to redis 
q.count() # activation function 
