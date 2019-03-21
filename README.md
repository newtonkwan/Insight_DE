# Great Work

Words move people. 

## Motivation 
The right words make all the difference.

Great Work is a text-based similarity pipeline for 45 million academic research papers that presents abstracts with the highest citations and tests their similarity using a code plagarising detector so you can answer questions such as: 

1) What do abstracts from the best papers look like? 
2) How can I write my abstract so more people will read my paper? 
3) What are the historical trends of the abstracts from the best papers? 

This project is inspired by local Seattle company, Textio -- a company focused on augmented writing for job descriptions. 

## Project description 
I will store research papers from Semantic scholar and arXiv [total 250 GB] into AWS S3. I will join the two data sets using Spark and extract the abstract and number of citations. 

For the top 10 papers, I will then compare each abstract to each of the others through the Levenshtein distance O(n!). This method was invented in 1965 by the Russian Mathematician Vladimir Levenshtein (1935-2017). The distance value describes the minimal number of deletions, insertions, or substitutions that are required to transform one string (the source) into another (the target). 

Ex. Levenshtein distance of "test" to "text" is 1 (one substitution). 

This calculation provides information on the similarity of these abstracts. If the abstracts are similar, abstracts could be a good indicator of number of citations. If they're not very similar, then a) maybe abstracts aren't a good indicator for citations; b) the domains of the papers are not similar enough; c) the similarity algorithm is not good enough. alidated by comparing abstracts from papers with a low and high number of citations.

When new research papers come in, I will use Spark Streaming. (not completely necessary since arXiv only updates monthly, but will be crucical for companies that get new data daily) 

## Tech Stack
AWS S3 [Storing the data]
Kafka [Ingestion]
Spark [Batch Processing]
Redis (or ElasticSearch) [Database; good for storing and searching text data]
Flask [Web]

## Data Source
Semantic Scholar: CS, Neuroscience, biomedical [46GB] [direct download] [.txt files] 
arXiv [190GB] [Amazon S3] [source files in TeX/LaTeX]

## Engineering Challenge
Combining two or more large data sets 
Extracting the abstract and number of citations from each paper
Streaming data when new papers come in [updated monthly in ArXiv]

## Business Value
For example, Textio is a Seattle company focused on augmented writing for job descriptions: how do you write a good job description so that you have a higher probability of getting good talent? New job postings are put up every day on Indeed, LinkedIn, Glassdoor, etc., so you need real time streaming 

## MVP
Join the two datasets together, extract the top 5 abstracts, and compute the Levenshtein distance for the top 20.  

## Stretch Goals
Add more research papers
Validate and implement a more sophisticated similarity system
Store abstracts by field and display top 5 abstracts for each field 
Compute Levenshtein distance for abstracts within the same category (Ex. CS, Biomedical, Neuroscience)




