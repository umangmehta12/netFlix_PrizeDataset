#Exploring NetFlix Prize Dataset
Our objective is to perform three main analysis in this project. 

1.	Identify the top 5 movies of each year

    a.	Analysis task: For the years ranging from 1890 to 2005, we aim to identify the top 5 movies of a year based on the average rating of each movie. This would be an interesting task as we hope to see which were the best movies released during different years. Years closer to the 19th century, with more movie releases, would reveal some interesting competition while movies in the earlier 18th century would reveal classics that are still considered worthy by an audience that rated them from1998 to 2005. 

    b.	Main task: Using Hive, we join the data set, perform the analysis task and compare the performance in plain Java MapReduce

    c.	Helper task: Build HBase tables to perform Hive operation

2.	Create a recommendation system

    a.	Analysis task: We aim to create a recommendation system based on clusters of movies. We would cluster movies based on the average rating. We believe this is interesting as we are basically creating a recommendation system. The audience would have the opportunity to select movies to view based on different ratings. Some of us do enjoy watching a “1 star” movie just to critique it. 

    b.	Main task: Using K-mean clustering we aim to create multiple clusters concurrently in a MapReduce program

3.	Gauge audience response in release year

    a.	Analysis task: For the release year of the movie, we would gauge the audience response received for the movie irrespective the rating received. This would show the “opening strength” of a movie and is irrespective of how users reviewed it over the years. 

    b.	Main task: Comparative performance analysis of HBase and PigLatin

    c.	Helper task: Design HBase tables and MapReduce program for the same
