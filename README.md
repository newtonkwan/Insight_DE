# Velma 

Words move people. 

## Motivation 
The right words make all the difference. Your paper's abstract or summary is the gateway to your work; it likely the single largest deciding factor in whether a reader will continue to read your paper. We believe in learning by example. 

Velma is a pipeline for 45 million academic research papers that provides the best example of abstracts based on: 

1) Number of citations 
2) Year 
3) Similarity to abstracts from other top papers in your field 

If you've convinced your reader to continue past the abstract, our work is done and yours is just starting.  

## Project description 
We use a lot of papers. Papers are collected from the Open Research Corpus and arXiv (~45 million, ~250 GB) from three main categories -- CS, neuroscience, biomedical -- and stored in AWS S3. The data sets are joined, abstracts are connected to their year and number of citations, and then sorted -- all using Spark. We consider a good paper one that has a high number of citations within its field. 

Most top papers have good abstracts. However, some papers are good in spite of bad abstracts. We want to provide abstract formats that are tried and true. Velma will take the top 10,000 papers from the last 5 years and compare them to one another through the Jiccard Index, providing a measure of word choice similarity. Velma will provide abstracts using the common language of your field, so you can be confident that you're using the right words.  

<a href="https://www.codecogs.com/eqnedit.php?latex=\fn_cm&space;\LARGE&space;\text{jaccard&space;index}&space;=&space;\frac{\text{words&space;shared&space;by&space;two&space;abstracts}}{\text{words&space;in&space;the&space;union&space;of&space;two&space;abstracts}}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?\fn_cm&space;\LARGE&space;\text{jaccard&space;index}&space;=&space;\frac{\text{words&space;shared&space;by&space;two&space;abstracts}}{\text{words&space;in&space;the&space;union&space;of&space;two&space;abstracts}}" title="\LARGE \text{jaccard index} = \frac{\text{words shared by two abstracts}}{\text{words in the union of two abstracts}}" /></a>

Staying up to date is also important. According to the NSF, it's estimated that around ~2 million new papers are published each year. That's why Velma supports streaming. Every month, when arXiv updates its collection of papers, Velma ingests and processes these new papers using Kafka and Spark Streaming. 

We store all of our data in Redis, which is great for text-based queries. To display Velma's top abstracts,  we use Flask as a simple web interfacing tool. 

## Tech Stack

![Alt text](./tech_stack_v3.png)

## Data Source
- Open Research Corpus: CS, Neuroscience, Biomedical [46GB] [direct download] [.txt files]

## Features 
### General
No user interaction required. Velma will look at the top 20% (by citation) of research papers available, compute the jaccard index on these top 20% papers, and display the 5 papers with the highest jaccard index. 

### AUTOMATIC UPDATES
Whenever the corpus updates, pull in the new data automatically and add it to the database. 

### MORE OR LESS 
The user will enter the title of a paper and Velma will provide 5 abstracts that are most like it. For example, the user enters "Mastering the Game of Go without Human Knowledge". Velma will search through the database for that paper. If it exists in the database, Velma will compute the jaccard index of the abstract to all other papers in the database, then display the name and abstracts of the top 5 abstracts that are most similar to it. 

### KEYWORD
The user will enter a keyword(s) that they want in their abstract. For example, the user inputs = "deep learning". Velma will scan through the database, pull every abstract with the keyword "deep learing", sort them by highest citation, take the top 20% of these papers, compute the jaccard index, and display the 5 papers with the highest jaccard index.  

### TAGGED 
A user will enter a tag(s) and Velma will provide the top 5 abstracts with those tags. For example, the user inputs the tag = "biomedical". Velma will look through only the abstracts with the tag = "biomedical", compute the jaccard index for the top 20% of papers and display the top 5. 

### YEAR BY YEAR
Allows the user to filter by year. Ex. only look at the papers from 2015-2019. 

## Engineering Challenge
- Extracting the abstract and number of citations from each paper
- Batching monthly when new papers come in

## Business Value
Words move people. Say your company is hiring and posts a job description to LinkedIn. Similar to abstracts, a job description is typically the first front between a company and a potential hire. There's a lot of jobs and a lot of people out there, so it's important for your company to communicate its job opening, mission, and values succinctly -- all of which are aimed to connect you to the right person for the job. The role of the abstract for researchers is no different. We could all gain from being a little clearer. 

As the world continues to grow, so will the amount of data in it. You'll want examples of what works, and Velma is your a real-time data collection and processing pipeline to help you do just that. 

Velma is made for academics, but choosing the right words is for everyone. 


## Stretch Goals
- Add more research papers from other data sources [Ask Neil]
- Scale the number of abstracts that can be compared efficiently 


## Appendix 
#### Other sorting algorithms 
- Sorensen-Dice: Find the common tokens, and divide it by the total number of tokens present by combining both sets
- Ratcliff-Obershelp similarity: Find the longest common substring from the two strings. Remove that part from both strings, and split at the same location. This breaks the strings into two parts, one left and another to the right of the found common substring. Now take the left part of both strings and call the function again to find the longest common substring. Do this too for the right part. This process is repeated recursively until the size of any broken part is less than a default value. Finally, a formulation similar to the above-mentioned dice is followed to compute the similarity score. The score is twice the number of characters found in common divided by the total number of characters in the two strings. 

## Credits 
Thank you to all the fellows in 19B for your help, without which this project would not have been possible. Special thanks to Sriram, Curtis, Hoa, and the rest of Insight Data Science staff. 


