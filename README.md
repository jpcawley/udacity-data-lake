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
