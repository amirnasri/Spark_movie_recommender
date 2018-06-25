# Summary
This repository hosts a movie recommendation system built using Apache Spark's MLLIB library, 
Amazon AWS EC2, and Django. 
In particular, it contains scripts to lunch an AWS EC2 cluster, run Apache Spark on each cluster node, and
execute a driver program on the cluster. The driver program analyzes movie ratings using the ALS algorithm
provided in the Spark MLLIB library and provides a movie-to-movie similarity matrix. This similarity matrix
is uploaded by the script to a web application where it is used to recommend movies that are similar to the 
movies input by the user.


## Description
Developed a movie recommendation system in Python using Apache Spark’s MLLIB library. Developed a

web application for the recommender using Django. The user can input up to three movies and is recom-
mended six movies along with a short description for each movie. The web application can be accessed at

http://amirnasri.ca/recommender/index.
• Developed Python scripts to launch an AWS EC2 cluster and run Spark on each cluster node. The script then
copies the movie ratings data to the cluster and executes Spark recommender driver program on the cluster.
The resulting recommendation information is uploaded to the HTTP server where the web application is hosted
using REST API.
• Optimize the parameters of the recommendation algorithm using cross-validation leading to more that 30%
reduction in the MMSE error.
