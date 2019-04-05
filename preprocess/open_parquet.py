import redis
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#reads parquet file from an AWS bucket
def read_parquet_from_bucket(bucket_name):
    return spark.read.parquet("s3a://{0}/*.parquet".format(bucket_name))

preprocessed_bucket_name = "preprocessed-open-research-corpus"
df = read_parquet_from_bucket(preprocessed_bucket_name)
rdb = redis.Redis(host = "10.0.0.5", port = 6379)

def check_rows(rows): 
    rdb = redis.Redis(host = "10.0.0.5", port = 6379)
    rdb.set(rows.id, rows.abstracts)

def store_to_redis(rdd):
    rdb = redis.Redis(host = "10.0.0.5", port = 6379)
    for row in rdd:
        rdb.set(rdd.id, rdd.abstracts)

#df.rdd.map(check_rows)
#store_to_redis(df.rdd)
print("Schema for raw data + ids + abstracts")
print("-------------------------------------")
df.createOrReplaceTempView("filtered_data")
df.printSchema()
results = spark.sql("SELECT * FROM filtered_data LIMIT 10")
print("First 10 entries for filtered data")
print("----------------------------")
results.show()
print()
print()
#print("Number of rows!", results.count())
print()
print()
