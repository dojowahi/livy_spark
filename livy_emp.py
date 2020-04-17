import logging
import sys
import pandas as pd
import pymysql
import json
from pyspark.sql import SparkSession


# logging configuration
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
root = logging.getLogger()
root.setLevel(logging.INFO)

appname = "livy_{0}_test"
parameters_dict = json.loads(sys.argv[1])
prefix = "git"

dep_list = parameters_dict['dep_list']

warehouse_location = "abspath('/tmp/warehouse/test/')"
spark = SparkSession.builder.appName(appname.format(prefix))\
    .config("spark.sql.warehouse.dir", warehouse_location)\
    .enableHiveSupport()\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


spark_query = """select name,dept,state from test.employee where department_list in ({0})"""
final_query = spark_query.format(dep_list)
df = spark.sql(final_query)

path = "s3://wahi-livy-out/emp"

df.write.format("orc").partitionBy('state').option("compression", "zlib").mode("append").save(t2_path)
