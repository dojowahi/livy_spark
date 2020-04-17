import utility.SparkLivy as livy
import json

livy_host = 'http://10.222.77.179:8998'

livy_conn = livy.SparkLivySql(livy_host)

spark_conf = {'spark.submit.deployMode': 'cluster', "spark.sql.autoBroadcastJoinThreshold": "4000000000", "spark.driver.memory":"12G", "spark.executor.cores":"2","spark.executor.memory":"12288M"}

spark_args_dict = {'dep_list':"'2U','2I'"}

partition_dict = json.dumps(spark_args_dict)


response = livy_conn.post_spark_job_submit(file="s3://aws-pyspark-job/livy_emp.py", args=[json.dumps(spark_args_dict)], conf=spark_conf)

batch_id = response['id']
batch_status = response['state']

if batch_status != 'starting':
    print("Livy submitted spark job but it is not started")

livy_conn.monitor_tier_process(batch_id)
