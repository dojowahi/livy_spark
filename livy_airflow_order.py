import logging
from pyspark.sql import SparkSession
import sys
from datetime import date
import json

# logging configuration
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
root = logging.getLogger()
root.setLevel(logging.INFO)

warehouse_location = "abspath('/tmp/warehouse/test/')"
spark = SparkSession.builder.appName("livy_order")\
    .enableHiveSupport()\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
#     .config("spark.memory.storageFraction", "0.01")\


parameters_dict = json.loads(sys.argv[1])

t2_max_execution_date = parameters_dict['extract_date']
print("Extract_dt",t2_max_execution_date)

t1_fact_sql = "select * from tmp.orders_new where hadoop_etl_load_date > '" + t2_max_execution_date + "'"
spark.sql(t1_fact_sql).createOrReplaceTempView('orders_delta')

t1_sql = """ select o.qty,o.price,r.state from orders_delta o, tmp.region r where o.r_code=r.r_code"""
spark.sql(t1_sql).createOrReplaceTempView('t1_df')
spark.catalog.cacheTable("t1_df")

today = date.today()
dir_suffix = today.strftime("%Y-%m-%d")

join_on = 't1.order_key = t2.order_key'
final_sql = "select * from t1_df union all (select /*+ BROADCAST(t1) */ t2.* " \
            "from history.orders_flat t2 left join t1_df t1 on ({0}))".format(join_on)
final_df = spark.sql(final_sql)

t2_new_path = "s3://wahi-job-output/qis_flat.db_drive_activity_flat"+dir_suffix
final_df.write.format("orc").partitionBy('order_date').option("compression", "zlib").mode("append").save(t2_new_path)
