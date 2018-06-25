# Summary
This repository hosts a movie recommendation system built using Apache Spark's MLLIB library, 
Amazon AWS EC2, and Django. 
In particular, it contains scripts to lunch an AWS EC2 cluster, run Apache Spark on each cluster node, and
execute a driver program on the cluster. The driver program analyzes movie ratings using the ALS algorithm
provided in the Spark MLLIB library and provides a movie-to-movie similarity matrix. This similarity matrix
is uploaded by the script to a web application hosted on an HTTP server. The web application receives three
movies input by the user and uses the movie similarity information to recommend new movies to the user.


## Description

