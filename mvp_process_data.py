import redis 
from pyspark import SparkContext 
'''
This file pulls 1GB .gz file of data from an S3 bucket
'''

rdb = redis.Redis(host="10.0.0.6", port=6379) # set up the redis database connection 

#filenames = "s3a://open-research-corpus/corpus-2019-01-31/s2-corpus-00.gz"
filenames = "s3a://open-research-corpus/sample-S2-records.gz" # path to the example file from S3 file 
sc = SparkContext(appName = "Pull Open Research Corpus") # setup the Spark Context 
data = sc.textFile(filenames) # reads and stores the data file 

len_of_data = data.count() # length of the RDD 

paper_mappings = {} # set up the dictionary to store information 

# setting up important variables 
parenthesis = "\"" # string literal for "
bracket = r"]" # look for "]"
paper_id_tag = "\"id\"" # find the first occurence of "id"
paper_year_tag = "\"year\"" # find the first occurence of "year"
paper_citation_tag = "\"inCitations\"" # find the occurence of "inCitations"
paper_abstract_tag = "\"paperAbstract\""

num_in_db = 0 
for line in data.take(len_of_data):
	#line = line.encode('utf8') # encodes the unicode to ascii 
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
	
	if id_tag not in paper_mappings:
		paper_mappings[id_tag] = (year_tag, num_citations, abstract_tag)

	rdb.set(id_tag, paper_mappings[id_tag][2]) # set the key to abstract
	num_in_db += 1
	if num_in_db % 1000 == 0:
		print("Stored", num_in_db, "entries so far") 







