import utility.PrestoClient as presto
import utility.config as c
import utility.SparkLivy as livy
import sys
import time
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime,timedelta
import json
from airflow.models import Variable

livy_url = Variable.get("livy_url")

def get_max_hadoop_date(**kwargs):
    pc = presto.PrestoClient(c.presto_user, c.presto_pwd)
    pc.connect()
    presto_query ="""select max(hadoop_etl_load_date) as extract_date  from tmp.orders"""
    hadoop_date = pc.fetch(presto_query)
    presto_date = hadoop_date['extract_date'][0]
    pc.disconnect()
    presto_dt_str = presto_date.split('.')
    print(presto_dt_str)
    datetimeobj = datetime.strptime(presto_dt_str[0], '%Y-%m-%d %H:%M:%S')
    extract_dt = datetimeobj - timedelta(hours=12)
    spark_date = extract_dt.strftime("%Y-%m-%d %H:%M:%S")
    return {'extract_date' : spark_date}


def submit_spark(**kwargs):
    spark_args_dict = []
    spark_config = {"spark.driver.memory": "30G", "spark.hadoop.orc.overwrite.output.file":"true", "spark.executor.memory":"20g", "spark.executor.cores":"5", "spark.hadoop.fs.s3.maxretries":"20", "spark.sql.autoBroadcastJoinThreshold":"1048576000","spark.sql.broadcastTimeout":"36000", "spark.driver.maxResultSize":"8GB","spark.memory.storageFraction":"0.2","spark.task.maxFailures""100000","spark.sql.shuffle.partitions": "400"}
    ti = kwargs['ti']
    spark_extract_date = ti.xcom_pull(key=None, task_ids='Get_Extract_Date')
    ext_dt = spark_extract_date['extract_date']
    print("The date passed to spark is",ext_dt)
    spark_args_dict = {'extract_date': ext_dt}

    livy_client = livy.SparkLivySql(livy_url)
    response = livy_client.post_spark_job_submit(file="s3://wahi-pyspark-jobs/livy_order.py",args=[json.dumps(spark_args_dict)],conf=spark_config)
    batch_id = response['id']
    batch_status = response['state']

    if batch_status != 'starting':
        print("Livy submitted spark job but it is not started")

    query_status = livy_client.monitor_tier_process(batch_id)
    if query_status:
        print("Setup is complete")
    else:
        print("Initial setup failed")
        sys.exit(1)


default_args = {
  'owner': '726084',
  'depends_on_past': False,
  'email': ['ankur.wahi@seagate.com'],
  'email_on_success':True,
  'email_on_failure': True,
  'retries': 0
}

dag = DAG("Order_livy_chk", description="Example shows presto,spark,livy and airflow", max_active_runs=1, catchup=False,default_args=default_args, start_date=datetime(2019, 12, 30),schedule_interval='15 12 * * FRI')

task_dummy = DummyOperator(task_id="dummy", dag=dag)

get_extract_date = PythonOperator(
    task_id='Get_Extract_Date',
    provide_context=True,
    dag=dag,
    python_callable=get_max_hadoop_date)

run_spark_job = PythonOperator(
    task_id='Run_Spark',
    provide_context=True,
    dag=dag,
    python_callable=submit_spark)

task_dummy >> get_extract_date >> run_spark_job
