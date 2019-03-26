# Newton Kwan
# March 25, 2019 
# Velma: Insight Data Engineering 
#################################
# This file extracts a sample of the data from Open Research Corpus and extracts citation, year, 
# and abstract from each paper 
#################################
#Attributes
#id  string: S2 generated research paper ID
#title  string: Research paper title.
#paperAbstract  string: Extracted abstract of the paper
#entities  list: S2 extracted list of relevant entities or topics.
#s2Url  string: URL to S2 research paper details page
#s2PdfUrl  string: URL to PDF on S2 if available.
#pdfUrls  list: URLs related to this PDF scraped from the web.
#authors  list: List of authors with an S2 generated author ID and name.
#inCitations  list: List of S2 paperId's which cited this paper.
#outCitations  list: List of paperId's which this paper cited.
#year  int: Year this paper was published as integer.
#venue  string: Extracted venue published.
#journalName  string: Name of the journal that published this paper.
#journalVolume  string: The volume of the journal where this paper was published.
#journalPages  string: The pages of the journal where this paper was published.
#sources  list: Identifies papers sourced from DBLP or Medline.
#doi  string: Digital Object Identifier registered at doi.org.
#doiUrl  string: DOI link for registered objects.
#pmid  string: Unique identifier used by PubMed.

import os

# Data cleaning 
cwd = os.getcwd() # get the current working directory 
with open(cwd + '/sample-S2-records.txt') as myfile:
	data = myfile.read().splitlines() # a list with each line as an entry


