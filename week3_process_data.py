import redis 
from pyspark.sql.types import IntegerType, ArrayType
from pyspark import SparkContext 
'''
This file pulls 1GB .gz file of data from an S3 bucket
'''
rdb = redis.Redis(host="10.0.0.6", port=6379) # set up the redis database connection 
# large 1GB file 
filenames = "s3a://open-research-corpus/corpus-2019-01-31/s2-corpus-00.gz"
#filenames = "s3a://open-research-corpus/sample-S2-records.gz" # path to the example file from S3 file 
# uncomment sc when you are running a spark-submit job on the terminal 
sc = SparkContext(appName = "Pull Open Research Corpus") # setup the Spark Context 
data = sc.textFile(filenames) # reads and stores the data file

def store_to_redis(rdd): 
    '''
    takes a rdd and stores relevant information to redis database
    '''
    papers = rdd.collect() # an iterable list 
    rdb = redis.Redis(host="10.0.0.6", port=6379) # set up the redis database connection 
    # setting the keywords that we search for
    parenthesis = "\"" # string literal for "
    bracket = r"]" # look for "]"
    paper_id_tag = "\"id\"" # find the first occurence of "id"
    paper_year_tag = "\"year\"" # find the first occurence of "year"
    paper_citation_tag = "\"inCitations\"" # find the occurence of "inCitations"
    paper_abstract_tag = "\"paperAbstract\""
    
    for paper in papers: 
        # look for the labels "id", "year", "inCitations", and "paperAbstract"
        id_label_start = line.find(paper_id_tag) # this is the index that the id label starts
        year_label_start = line.find(paper_year_tag) # index that the year label starts 
        citation_label_start = line.find(paper_citation_tag) # index that the citation label starts 
        abstract_label_start = line.find(paper_abstract_tag) # index that the abstrat label starts 

        # look for the tag of each label 
        id_tag_start = id_label_start + 6 # this is the index that the id tag starts. Always be 6.
        year_tag_start = year_label_start + 7 # index that the year tag starts 
        citation_tag_start = citation_label_start + 15 # index that the citation tag starts
        abstract_tag_start = abstract_label_start + 17 # the start of the abstract tag 

        # look for the last index of each tag 
        id_tag_end = line.find(parenthesis, id_tag_start)  #this is the index that the id tag ends
        year_tag_end = line.find(parenthesis, year_tag_start) - 1 # this is the index that the year tag ends
        citation_tag_end = line.find(bracket, citation_tag_start)  # this is the index that the citation tag ends 
        abstract_tag_end = line.find(parenthesis, abstract_tag_start) # the end of the abstract tag

        #extract the tag
        #id_tag = line[id_tag_start:id_tag_end] # id tag string 
        #year_tag = line[year_tag_start:year_tag_end] # year tag string
        #citation_list = line[citation_tag_start:citation_tag_end].split(",") # make it a list, count number of entries
        #num_citations = len(citation_list) # number of citations 
        #abstract_tag = line[abstract_tag_start:abstract_tag_end] # abstract tag string

        # computation 
        id_tag = line[id_tag_start:id_tag_end] # id tag string 
        if year_label_start == -1: # it didn't find the string year
            year_tag = None
        else:
            year_tag = line[year_tag_start:year_tag_end] # year tag string
        if citation_tag_start == citation_tag_end: # if there are no citations:
            num_citations = 0
        else:
            citation_list = line[citation_tag_start:citation_tag_end].split(",") # make it a list, count number of entries
            num_citations = len(citation_list) # number of citations
        abstract_tag = line[abstract_tag_start:abstract_tag_end] # abstract tag string
        # this serializes the data using json.dumps so that it can be put into redis 
        #info = {"year":year_tag, "citations":num_citations, "abstract":abstract_tag}
        #json_representation = json.dumps(info)
        #rdb.set(id_tag, json_representation) # push to redis database 
        rdb.set(id_tag, abstract_tag)

rdd = data.flatMap(lambda line: line.split("\n")).map(store_to_redis(rdd))

