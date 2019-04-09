# Velma 

Words move people. 

## Motivation 
The right words make all the difference. Your paper's abstract or summary is the gateway to your work; it likely the single largest deciding factor in whether a reader will continue to read your paper. We believe in learning by example. 

Velma is a pipeline for 45 million academic research papers that provides examples of high quality abstracts from top papers based on: 

1) Number of citations 
2) Tags 
3) Similarity to abstracts from other top papers in your field 

Give Velma the title of a paper that you think has a good abstract, and we'll provide you with 5 additional abstracts from top papers so you can learn and write by example. If you've convinced your reader to continue past the abstract, our work is done and yours is just starting.  

## Project description 
We use a lot of papers. Papers are collected from the Open Research Corpus (~45 million) from three main categories -- CS, neuroscience, biomedical -- and stored in AWS S3. The abstracts are connected to their number of citations and tags, then sorted -- all using Spark. We consider a good paper one that has a high number of citations within its field. 

Most top papers have good abstracts. Through data driven filtering, Velma will only look at papers with more than 20 citations and relevant tags when comparing abstracts to one another through the Jaccard Index, providing a measure of word choice similarity. Velma will provide abstracts using the common language of your field, so you can be confident that you're using the right words.  

<a href="https://www.codecogs.com/eqnedit.php?latex=\fn_cm&space;\LARGE&space;\text{jaccard&space;index}&space;=&space;\frac{\text{words&space;shared&space;by&space;two&space;abstracts}}{\text{words&space;in&space;the&space;union&space;of&space;two&space;abstracts}}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?\fn_cm&space;\LARGE&space;\text{jaccard&space;index}&space;=&space;\frac{\text{words&space;shared&space;by&space;two&space;abstracts}}{\text{words&space;in&space;the&space;union&space;of&space;two&space;abstracts}}" title="\LARGE \text{jaccard index} = \frac{\text{words shared by two abstracts}}{\text{words in the union of two abstracts}}" /></a>

Staying up to date is also important. According to the NSF, it's estimated that around ~2 million new papers are published each year. Every month, when updates are made to the collection of papers, Velma can ingest and processes these papers again.  

We store our data in Redis, which is great for text-based queries. To display Velma's abstracts, we use Flask as a simple web interfacing tool. 

## Tech Stack

![Alt text](./pictures/tech_stack_v3.png)

## Data Source
- Open Research Corpus: CS, Neuroscience, Biomedical [46GB] [direct download] [.txt files]

## Features 

### Gimmie a title 
The user will enter the title of a paper and Velma will provide not only the abstract from that paper but 5 additional abstracts that are most similar to it. For example, the user enters "Mastering the game of Go without human knowledge". Velma will search through the database for that paper. If it exists in the database, Velma display 5 additional abstracts that are most similar to the abstract from "Mastering the game of Go without human knowledge". 

## Engineering Challenge
Comparing the jaccard index of each paper to every other paper is O(n^2). With 45,000,000 research papers, this task is impossible. So what next? Taking a closer look at the data, we can determined three filters that not only reduced the number of computations but also increased the quality of the abstracts 

### Abstracts 
- The data isn't very clean. Roughly 35% of papers from the data did not contain abstracts. 

### Citations 
- Roughly 90% of papers have less than 20 citations. Since we're in the business of top papers, we cut away 90% of the papers. 

### Tags 
- Some tags are more relevant than others. Only about 1% of tags are found on more than 1000 papers. We will only look at papers that contain at least one of these tags. This way, most papers will find themselves generally closer to each other. 


## Business Value
Words move people. Say your company is hiring and posts a job description to LinkedIn. Similar to abstracts, a job description is typically the first front between a company and a potential hire. There's a lot of jobs and a lot of people out there, so it's important for your company to communicate its job opening, mission, and values succinctly -- all of which are aimed to connect you to the right person for the job. The role of the abstract for researchers is no different. We could all gain from being a little clearer. 

As the world continues to grow, so will the amount of data in it. You'll want examples of what works, and Velma is your a real-time data collection and processing pipeline to help you do just that. 

Velma is made for academics, but choosing the right words is for everyone. 


## Future features 

### KEYWORD
The user will enter a keyword(s) that they want in their abstract. For example, the user inputs = "deep learning". Velma will scan through the database, pull every abstract with the keyword "deep learing", sort them by highest citation, take the top 20% of these papers, compute the jaccard index, and display the 5 papers with the highest jaccard index.  

### TAGGED 
A user will enter a tag(s) and Velma will provide the top 5 abstracts with those tags. For example, the user inputs the tag = "biomedical". Velma will look through only the abstracts with the tag = "biomedical", compute the jaccard index for the top 20% of papers and display the top 5. 

### YEAR BY YEAR
Allows the user to filter by year. Ex. only look at the papers from 2015-2019. 


## Credits 
Thank you to all the fellows in 19A for your help, without which this project would not have been possible. Special thanks to Sriram, Curtis, Hoa, and the rest of Insight Data Science staff. 


