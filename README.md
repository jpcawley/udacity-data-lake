# Sparkify Data Lake
## Introduction
A music streaming startup, Sparkify, wants to analyze the [data](http://millionsongdataset.com/) they've been collecting on songs and user activity on their new music streaming app. They have grown their user base and song database and want to move their data warehouse to a data lake. Their data - a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app - resides in S3.
## Project Objective
Build an ETL pipeline that extracts their data from S3, processes it with Spark, and loads it back into S3 as a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. This Spark process will be on a cluster in AWS.
## Contents
1. etl.py: Python file with functions to run ETL processing using PySpark
2. dl.cfg: Configuration file which holds AWS credentials
3. Data Lake Schema 
4. ReadMe
## Schema
![ERD](Sparkify_ERD.png)
## Prerequisites
1. Python 3.6.x or newer
2. AWS CLIV2
3. SSH
4. An active AWS EMR Cluster in us-west-2 with 5 instances
5. An active S3 bucket in us-west-2
6. Private key file (inyour workspace)
## Python Library for EMR Cluster
* configparser
* datetime
## How to Run
1. Add AWS credentials to dl.cfg (with quotes)
2. Add bucket name to line 169 in etl.py
3. Ensure you can connect to your EMR cluster (run ssh)
```
ssh -i <path to private key file> hadoop@<master node host>
```
Once connected, type 
```
logout
```
In your root directory, copy `etl.py`, `dl.cfg`, and `<privatekey>.pem` file to hadoop
```
scp -i <path to private key file> <path to private key file> hadoop@<master node host>:/home/hadoop/
```
```
scp -i <path to private key file> etl.py hadoop@<master node host>:/home/hadoop/
```
```
scp -i <path to private key file> dl.cfg hadoop@<master node host>:/home/hadoop/
```
Install requirements using `pip install --user <package>`
