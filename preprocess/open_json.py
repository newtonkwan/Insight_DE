from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#reads all JSON files from an AWS bucket
def read_all_json_from_bucket(bucket_name):
    return spark.read.json("s3a://{0}/*.json*".format(bucket_name))

preprocessed_bucket_name = "preprocessed-open-research-corpus"
df = read_all_json_from_bucket(preprocessed_bucket_name)

print("Schema for raw data + ids + abstracts")
print("-------------------------------------")
df.createOrReplaceTempView("raw_ids_and_abstracts")
df.printSchema()
results = spark.sql("SELECT * FROM raw_ids_and_abstracts")
print("First 5 entries for add_ids_abstracts data")
print("----------------------------")
results.show()
print()
print()
print("Number of rows!", results.count())
print()
print()
