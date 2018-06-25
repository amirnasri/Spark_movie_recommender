# Summary
This repository hosts a movie recommendation system built using Apache Spark's MLLIB library, 
Amazon AWS EC2, and Django. 

In particular, it contains scripts to lunch an AWS EC2 cluster, run Apache Spark on each cluster node, and
execute a driver program on the cluster. The driver program analyzes movie ratings using the ALS algorithm
provided in the Spark MLLIB library and provides a movie-to-movie similarity matrix. This similarity matrix
is uploaded by the script to a web application hosted on an HTTP server. The web application receives three
movies input by the user and uses the movie similarity information to recommend new movies to the user.


## Description
The repository contains two main directories `python` and `web`. `python` directory contains scripts for
launching and preparing the AWS cluster, as well as the Spark driver program. `web` directory contains 
Django files for the web application.

The following is a description of the main scripts in these folders.

### `python/spark.py:`
This script lunch an AWS EC2 cluster, run Apache Spark on each cluster node, and
execute a driver program on the cluster. It takes several arguments such as the 
Spark driver program, AWS access key information, and number of cluster nodes to run.
Here is a description of the input arguments:

```
./spark.py --help
usage: spark.py [-h] [--remote-spark-home REMOTE_SPARK_HOME] [--copy-master]
                [--copy-dir COPY_DIR] [-u USER] [-i IDENTITY_FILE]
                [--ec2-access-key EC2_ACCESS_KEY] [-s SLAVES] [-k KEY_PAIR]
                [-t INSTANCE_TYPE] [-r REGION] [--private-ips] [-a AMI]
                [--remote-server REMOTE_SERVER] [--run-local]
                {launch,destroy,start,start-spark,stop-spark} cluster_name
                python_driver

positional arguments:
  {launch,destroy,start,start-spark,stop-spark}
                        Action to perform on the cluster.
  cluster_name          Name of the ec2 culster.
  python_driver         Python driver program to run on the master.

optional arguments:
  -h, --help            show this help message and exit
  --remote-spark-home REMOTE_SPARK_HOME
                        Directory on the instances where spark is installed
                        (default: '~/spark/').
  --copy-master         Whether to copy 'copy-dir' to the master.
  --copy-dir COPY_DIR   Local directory to be copied to the master (default:
                        'work').
  -u USER, --user USER  SSH user to use for logging onto the master (default:
                        'root').
  -i IDENTITY_FILE, --identity-file IDENTITY_FILE
                        SSH private key file to user for logging into
                        instances (default: 'None').
  --ec2-access-key EC2_ACCESS_KEY
                        AWS ec2 access key file (default: 'accessKeys.csv').
  -s SLAVES, --slaves SLAVES
                        Number of slaves to launch (default: 1)
  -k KEY_PAIR, --key-pair KEY_PAIR
                        Key pair to use on instances (default: '').
  -t INSTANCE_TYPE, --instance-type INSTANCE_TYPE
                        Type of instance to launch (default: t2.micro).
  -r REGION, --region REGION
                        EC2 region used to launch instances in, or to find
                        them in (default: us-east-1
  --private-ips         Use private IPs for instances rather than public if
                        VPC/subnet requires that.
  -a AMI, --ami AMI     Amazon Machine Image ID to use (default: ami-
                        52d5d044).
  --remote-server REMOTE_SERVER
                        Remote server address where the results are uploaded
                        (default: amirnasri.ca).
  --run-local           run driver program in local machine instead of the
                        remote server
```
