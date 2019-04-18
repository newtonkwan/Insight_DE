'''
Newton Kwan
April 13, 2019
Velma

This creates a dataframe with jaccard index comparison 
'''
import pyspark
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import udf, struct, col, rank
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
import boto3
import nltk
from nltk.stem import PorterStemmer
import redis 

conf = (SparkConf().set("spark.driver.maxResultSize", "32g"))

# Create new context
#sc = SparkContext(conf=conf)

sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

jaccard_index_udf = udf(lambda line:jaccard_index(line), StringType())

def tokenizer(abstract):
    '''
    This function takes a string and makes it a list of words 
    '''
    token_abstract = abstract.split()
    return token_abstract

def stemming(list_abstract):
    '''
    This function takes a tokenized string of words and stems them 
    using the PorterStemmer from the nltk package 
    '''
    stem_sentence=[]
    for word in list_abstract:
        stem_sentence.append(PorterStemmer().stem(word))
    return stem_sentence

def add_jaccard(df):
    df = df.withColumn("jaccard", jaccard_index_udf(struct([df[x] for x in df.columns])))
    return df 

def jaccard_index(line):
    '''
    Calculates the jaccard index of two abstracts 
    '''

    jiccard_index = None #initialize jiccard index 
    abstract_1 = line.abstracts_df1
    abstract_2 = line.abstracts_df2
     
    # splits the abstracts into a list of words  
    token_abst_1 = tokenizer(abstract_1) # this is a list of tokens from abstract 1
    token_abst_2 = tokenizer(abstract_2) # this is a list of tokens from abstract 2
    
    # stem the words 
    stemmed_abst_1 = set(stemming(token_abst_1)) # this is set of stemmed tokens from abstract 1 
    stemmed_abst_2 =  set(stemming(token_abst_2)) # this is a set of stemmed tokens from abstract 2 

    # count number of words in each list 
    #words_in_abst_1 = len(token_abst_1)# number of words in abstract 1
    #words_in_abst_2 = len(token_abst_2) # number of words in abstract 2 

    # combines the two lists and takes the set to get total number of unique words 
    combined_set = stemmed_abst_1.union(stemmed_abst_2)
    
    # Find the length of the set 
    words_in_combined_set = len(combined_set) # number of words in combined set
    
    same_words = 0 # initialize the number of same words 
    for word in stemmed_abst_1: 
        if word in stemmed_abst_2:
            same_words +=1 

    jaccard_index = same_words / words_in_combined_set
    
    return jaccard_index 

def read_from_s3():
    '''
    This function reads a parquet file from an S3 bucket 
    '''
    df = spark.read.load("s3a://preprocessed-open-research-corpus/filtered-dataframes/part-*")
    return df

def outer_join(df):
    '''
    does an outer join
    '''
    df1 = df.alias("df1")
    df2 = df.alias("df2")
    df1_r = df1.select(*(col(x).alias(x + '_df1') for x in df1.columns if x in ['id','abstracts','tags', 'citations']))
    df2_r = df2.select(*(col(x).alias(x + '_df2') for x in df2.columns if x in ['id','abstracts','tags', 'citations']))
    cond = [df1_r.id_df1 != df2_r.id_df2, df2_r.citations_df2 > 25]
    outer_join_df = df1_r.join(df2_r, cond, how='left')
    outer_join_df = outer_join_df.withColumn("Keep", check_tag_udf(struct([outer_join_df[x] for x in outer_join_df.columns])))
    outer_join_df = outer_join_df.filter(col('Keep') == True)
    return outer_join_df

def check_sets(line):
    '''
    Checks the set membership of the source tags and target tags and return True if 
    a tag in the source set also appears in the target set
    '''
    
    in_set = False # initialiaze this to False 
    tag_source_set = set(line.tags_df1) # creates a set ouf of the tag list from tags_df1
    tag_target_set = set(line.tags_df2) # creates a set out of the tag list from tags_df2    
    
    # check if a word in source set is a member of target set 
    for word in tag_source_set:
        if word in tag_target_set:
            in_set = True 
    
    return in_set

check_tag_udf = udf(lambda line: check_sets(line), BooleanType())

def drop_unneeded_part_1(df):
    '''
    drop unneeded columns after computing jaccard 
    '''
    df = df.drop(df.tags_df1)
    df = df.drop(df.tags_df2)
    df = df.drop(df.citations_df1)
    df = df.drop(df.citations_df2)
    df = df.drop(df.abstracts_df1)
    df = df.drop(df.abstracts_df2)
    df = df.drop(df.Keep)
    
    return df

def drop_unneeded_part_2(df):
    '''
    drop uneeded columns after ranking top 5 jaccard
    '''
    df = df.drop(df.jaccard)
    df = df.drop(df.rank)
    return df

def store_in_s3(df, filename):
    '''
    This file takes a dataframe and appends it into existing dataframe that is in S3 
    '''
    df.write.save("s3a://preprocessed-open-research-corpus/{0}".format(filename), format="parquet", mode="append")
    
    return 1

def store_to_redis_part_1(line):
    '''
    Take a line from the dataframe and store it into redis
    This doesn't need to return anything, but spark requires it 
    '''
    rdb = redis.Redis(host="10.0.0.5", port="6379")
    rdb.set(line['title'], line['id'])
    rdb.lpush(line['id'],line['title'])
    rdb.lpush(line['id'],line['abstracts'])
    return line

def store_to_redis_part_2(line):
    '''
    Take a line from the dataframe and store it into redis
    This doesn't need to return anything, but spark requires it 
    '''
    #rdb = redis.Redis(host="10.0.0.5", port="6379")
    rdb = redis.Redis(host="10.0.0.5", port="6379")
    rdb.lpush(line['id_df1'],line['id_df2'])
    return line

filename = "outer_joined_filtered"
df = read_from_s3()
#print("Number of rows", df.count())
#q = df.rdd.map(store_to_redis_part_1) # dummy variable name to store to redis 
#q.count() # activation function
df = outer_join(df)
df = add_jaccard(df)
df = drop_unneeded_part_1(df)
window = Window.partitionBy(df['id_df1']).orderBy(df['jaccard'].desc())
df = df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)
df = drop_unneeded_part_2(df).repartition("id_df1")
#print("Num of rows", df.count())
#store_in_s3(df, filename)
df.foreachPartition(store_to_redis_part_2)
#q = df.rdd.map(store_to_redis_part_2) # dummy variable name to store to redis 
#q.count() # activation function




