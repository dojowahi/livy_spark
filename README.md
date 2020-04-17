###Pre-requisite
A spark cluster with Apache Livy running. 
The code in this repository has been tested on AWS EMR cluster for jobs which run from 30 min to 24 hrs.

## Objective
Examples on how to submit spark jobs through Apache Livy

### SparkLivy.py
This is the main piece fo code which creates,monitors and deletes livy session

### PrestoClient.py
This code is used to create a presto connection and then execute queries

### config.py
Stores user credentails

### pyspark_submitjob.py
Shows an example of how to connect to Livy and submit a job

### livy_emp.py
This is the pyspark code which is called in pyspark_submitjob.py and submitted to a spark livy