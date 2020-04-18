### Pre-requisite
An AWS EMR cluster with Apache Livy and Apache Spark running. 
Access to S3 buckets, where files can be stored and Spark job output can be written
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
This is the pyspark code which is called in pyspark_submitjob.py and submitted to a spark livy. The file is stored in a S3 bucket and ouput of this job is also stored iin a bucket.

### airflow_livy_spark_presto_dag.py
Example of an airflow dag which cshows an ETL flow of connecting to Presto, where a query is executed and a date is returned. The date is passed to another task via XCOM, where it is passed as an arguement to a Spark job which is then submitted to Livy

### livy_airflow_order.py
This ia a pyspark job which is submitted to Livy via the airflow dag
