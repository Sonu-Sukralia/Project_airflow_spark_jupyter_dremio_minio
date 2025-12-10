# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.sensors.filesystem import FileSensor
# from airflow.operators.python import PythonOperator
# import logging

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def log_file_detected(**context):
#     """Log when file is detected"""
#     logging.info("=" * 60)
#     logging.info("📁 New file detected in source folder!")
#     logging.info("🚀 Triggering Spark job...")
#     logging.info("=" * 60)

# def log_completion(**context):
#     """Log completion"""
#     logging.info("=" * 60)
#     logging.info("✅ Pipeline Completed Successfully!")
#     logging.info("📊 Check History Server: http://localhost:18080")
#     logging.info("💾 Check MinIO target folder: http://localhost:9001")
#     logging.info("=" * 60)

# with DAG(
#     's3_file_processing_pipeline',
#     default_args=default_args,
#     description='Trigger Spark job when file arrives in S3 source folder',
#     schedule_interval='@hourly',  # Check every hour
#     catchup=False,
#     tags=['spark', 's3', 'file-trigger'],
# ) as dag:

#     # Task 1: Wait for file in source folder
#     wait_for_file = FileSensor(
#         task_id='wait_for_source_file',
#         filepath='/opt/airflow/data/source/*.json',  # Local path that monitors S3
#         fs_conn_id='fs_default',
#         poke_interval=30,  # Check every 30 seconds
#         timeout=3600,  # Wait up to 1 hour
#         mode='poke',
#     )

#     # Task 2: Log file detection
#     log_detection = PythonOperator(
#         task_id='log_file_detected',
#         python_callable=log_file_detected,
#     )

#     # Task 3: Process file with Spark
#     process_file = SparkSubmitOperator(
#         task_id='process_json_file',
#         conn_id='spark-conn',
#         application='/opt/airflow/jobs/s3_json_processor.py',
#         name='S3_File_Processor_{{ ts_nodash }}',
#         conf={
#             # Event logging for History Server
#             'spark.eventLog.enabled': 'true',
#             'spark.eventLog.dir': '/opt/spark-events',
#             'spark.history.fs.logDirectory': '/opt/spark-events',
            
#             # S3/MinIO credentials
#             'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
#             'spark.hadoop.fs.s3a.access.key': 'admin',
#             'spark.hadoop.fs.s3a.secret.key': 'password',
#             'spark.hadoop.fs.s3a.path.style.access': 'true',
#             'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
#         },
#     )

#     # Task 4: Log completion
#     log_done = PythonOperator(
#         task_id='log_completion',
#         python_callable=log_completion,
#     )

#     # Task dependencies
#     wait_for_file >> log_detection >> process_file >> log_done











# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.sensors.filesystem import FileSensor
# from airflow.operators.python import PythonOperator
# import logging

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def log_file_detected(**context):
#     """Log when file is detected"""
#     logging.info("=" * 60)
#     logging.info("📁 New file detected in source folder!")
#     logging.info("🚀 Triggering Spark job...")
#     logging.info("=" * 60)

# def log_completion(**context):
#     """Log completion"""
#     logging.info("=" * 60)
#     logging.info("✅ Pipeline Completed Successfully!")
#     logging.info("📊 Check History Server: http://localhost:18080")
#     logging.info("💾 Check MinIO target folder: http://localhost:9001")
#     logging.info("=" * 60)

# with DAG(
#     's3_file_processing_pipeline',
#     default_args=default_args,
#     description='Trigger Spark job when file arrives in S3 source folder',
#     schedule_interval='@hourly',  # Check every hour
#     catchup=False,
#     tags=['spark', 's3', 'file-trigger'],
# ) as dag:

#     # Task 1: Wait for file in source folder
#     wait_for_file = FileSensor(
#         task_id='wait_for_source_file',
#         filepath='/opt/airflow/data/source/*.json',  # Local path that monitors S3
#         fs_conn_id='fs_default',
#         poke_interval=30,  # Check every 30 seconds
#         timeout=3600,  # Wait up to 1 hour
#         mode='poke',
#     )

#     # Task 2: Log file detection
#     log_detection = PythonOperator(
#         task_id='log_file_detected',
#         python_callable=log_file_detected,
#     )

#     # Task 3: Process file with Spark
#     process_file = SparkSubmitOperator(
#         task_id='process_json_file',
#         conn_id='spark-conn',
#         application='/opt/airflow/jobs/s3_json_processor.py',
#         name='S3_File_Processor_{{ ts_nodash }}',
#         conf={
#             # Event logging for History Server
#             'spark.eventLog.enabled': 'true',
#             'spark.eventLog.dir': '/opt/spark-events',
#             'spark.history.fs.logDirectory': '/opt/spark-events',
            
#             # S3/MinIO credentials
#             'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
#             'spark.hadoop.fs.s3a.access.key': 'admin',
#             'spark.hadoop.fs.s3a.secret.key': 'password',
#             'spark.hadoop.fs.s3a.path.style.access': 'true',
#             'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
#         },
#     )

#     # Task 4: Log completion
#     log_done = PythonOperator(
#         task_id='log_completion',
#         python_callable=log_completion,
#     )

#     # Task dependencies
#     wait_for_file >> log_detection >> process_file >> log_done