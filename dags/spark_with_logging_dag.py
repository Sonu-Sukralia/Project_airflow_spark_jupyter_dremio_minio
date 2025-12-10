# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     'spark_job_with_history',
#     default_args=default_args,
#     description='Spark job that logs to History Server',
#     schedule_interval=None,
#     catchup=False,
#     tags=['spark', 'logging'],
# ) as dag:

#     # Using spark-submit to ensure logs go to History Server  /opt/bitnami/spark
#     submit_spark_job = BashOperator(
#         task_id='submit_spark_job',
#         bash_command="""
#         docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
#           --master spark://spark-master:7077 \
#           --deploy-mode client \
#           --name "Airflow_Triggered_Job_{{ ts_nodash }}" \
#           --conf spark.eventLog.enabled=true \
#           --conf spark.eventLog.dir=/opt/spark-events \
#           --conf spark.history.fs.logDirectory=/opt/spark-events \
#           /jobs/airflow_spark_job.py
#         """,
#     )

#     submit_spark_job








# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     'spark_job_with_history',
#     default_args=default_args,
#     description='Spark job that logs to History Server',
#     schedule_interval=None,
#     catchup=False,
#     tags=['spark', 'logging'],
# ) as dag:

#     submit_spark_job = SparkSubmitOperator(
#         task_id='submit_spark_job',
#         conn_id='spark-conn',
#         application='jobs/airflow_spark_job.py',
#         name='Airflow_Triggered_Job_{{ ts_nodash }}',
#         conf={
#             'spark.eventLog.enabled': 'true',
#             'spark.eventLog.dir': '/opt/spark-events',
#             'spark.history.fs.logDirectory': '/opt/spark-events',
#         },
#     )




from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_job_with_history',
    default_args=default_args,
    description='Spark job that logs to History Server',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'logging'],
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        conn_id='spark-conn',
        application='/opt/airflow/jobs/airflow_spark_job.py',
        name='Airflow_Triggered_Job_{{ ts_nodash }}',
        conf={
            # Event logging for History Server
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': '/opt/spark-events',
            'spark.history.fs.logDirectory': '/opt/spark-events',
            
            # S3/MinIO credentials
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'admin',
            'spark.hadoop.fs.s3a.secret.key': 'password',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        },
    )