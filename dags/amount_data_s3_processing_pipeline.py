"""
Airflow DAG: Amount Data S3 Processing Pipeline with File Timestamping
Purpose: 
1. Monitor MinIO for new JSON files in test/source/
2. Move file to test/source/filename_YYYYMMDD_HHMMSS (with timestamp)
3. Process only the LATEST file with Spark
4. Save results to warehouse bucket
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
import logging
import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# MinIO/S3 Configuration
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
SOURCE_BUCKET = "test"
SOURCE_PREFIX = "source"

def get_s3_client():
    """Create and return S3 client for MinIO"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1',
        use_ssl=False
    )

def check_for_new_json_files(**context):
    """
    Sensor function to check if new JSON files exist in MinIO source bucket
    (without timestamp suffix)
    Returns True if files found, False otherwise
    """
    try:
        s3_client = get_s3_client()
        
        # List objects in source folder
        response = s3_client.list_objects_v2(
            Bucket=SOURCE_BUCKET,
            Prefix=f"{SOURCE_PREFIX}/"
        )
        
        # Check if any JSON files exist WITHOUT timestamp (new files)
        if 'Contents' in response:
            json_files = [
                obj['Key'] for obj in response['Contents'] 
                if obj['Key'].endswith('.json') and '_' not in obj['Key'].split('/')[-1].replace('.json', '')
            ]
            
            if json_files:
                logger.info(f"✅ Found {len(json_files)} new JSON file(s) without timestamp")
                for file in json_files:
                    logger.info(f"   📄 {file}")
                return True
        
        logger.info("⏳ No new JSON files found (all files have timestamps)")
        return False
    
    except ClientError as e:
        logger.error(f"❌ MinIO connection error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error checking MinIO: {str(e)}")
        return False

def move_file_with_timestamp(**context):
    """
    Move the latest JSON file from test/source/filename.json 
    to test/source/filename_YYYYMMDD_HHMMSS.json
    Store the new filename in XCom for later tasks
    """
    try:
        s3_client = get_s3_client()
        
        logger.info("=" * 80)
        logger.info("📁 MOVING FILE WITH TIMESTAMP")
        logger.info("=" * 80)
        
        # List all JSON files in source folder
        response = s3_client.list_objects_v2(
            Bucket=SOURCE_BUCKET,
            Prefix=f"{SOURCE_PREFIX}/"
        )
        
        # Find files without timestamp (new files)
        if 'Contents' not in response:
            raise Exception("No files found in source folder")
        
        new_files = [
            obj for obj in response['Contents'] 
            if obj['Key'].endswith('.json') 
            and '_' not in obj['Key'].split('/')[-1].replace('.json', '')
        ]
        
        if not new_files:
            raise Exception("No new files found (all files already have timestamps)")
        
        # Get the latest file (most recently uploaded)
        latest_file = max(new_files, key=lambda x: x['LastModified'])
        original_key = latest_file['Key']
        original_filename = original_key.split('/')[-1]
        
        logger.info(f"📄 Latest file found: {original_key}")
        logger.info(f"   Upload time: {latest_file['LastModified']}")
        
        # Generate new filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_without_ext = original_filename.replace('.json', '')
        new_filename = f"{filename_without_ext}_{timestamp}.json"
        new_key = f"{SOURCE_PREFIX}/{new_filename}"
        
        logger.info(f"✏️  Renaming to: {new_key}")
        
        # Copy file with new name
        s3_client.copy_object(
            Bucket=SOURCE_BUCKET,
            CopySource={'Bucket': SOURCE_BUCKET, 'Key': original_key},
            Key=new_key
        )
        
        logger.info(f"✅ File copied with timestamp: {new_key}")
        
        # Delete original file
        s3_client.delete_object(Bucket=SOURCE_BUCKET, Key=original_key)
        logger.info(f"🗑️  Original file deleted: {original_key}")
        
        # Store the new filename in XCom for Spark job to use
        context['task_instance'].xcom_push(
            key='latest_file_key',
            value=new_key
        )
        context['task_instance'].xcom_push(
            key='latest_filename',
            value=new_filename
        )
        
        logger.info(f"📌 File info stored for Spark job")
        logger.info("=" * 80 + "\n")
        
        print("\n" + "=" * 80)
        print(f"  ✅ FILE MOVED SUCCESSFULLY WITH TIMESTAMP")
        print("=" * 80)
        print(f"  Original: {original_filename}")
        print(f"  New Name: {new_filename}")
        print(f"  Location: {SOURCE_PREFIX}/")
        print("=" * 80 + "\n")
        
        return True
    
    except Exception as e:
        logger.error(f"❌ Error moving file: {str(e)}")
        print(f"\n❌ ERROR: {str(e)}\n")
        raise

def log_pipeline_start(**context):
    """Log pipeline start message"""
    print("\n" + "=" * 80)
    print("  🚀 AMOUNT DATA PROCESSING PIPELINE STARTED")
    print("=" * 80)
    print(f"  📊 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  🪣 Source Bucket: {SOURCE_BUCKET}/source/")
    print(f"  🪣 Target Bucket: warehouse/processed/")
    print("=" * 80 + "\n")
    
    logging.info("✅ Pipeline initialization complete")

def log_file_detected(**context):
    """Log when files are detected"""
    # Get file info from XCom
    task_instance = context['task_instance']
    latest_file = task_instance.xcom_pull(
        task_ids='move_file_with_timestamp',
        key='latest_filename'
    )
    
    print("\n" + "=" * 80)
    print("  📁 JSON FILE DETECTED AND RENAMED!")
    print("=" * 80)
    print(f"  📄 File: {latest_file}")
    print(f"  📍 Location: {SOURCE_PREFIX}/")
    print("  🚀 Triggering Spark job for data processing...")
    print("  ⏳ Please wait, this may take a few minutes...")
    print("=" * 80 + "\n")
    
    logging.info(f"📁 File detection confirmed - launching Spark job for {latest_file}")

def log_spark_job_started(**context):
    """Log when Spark job starts"""
    task_instance = context['task_instance']
    latest_file = task_instance.xcom_pull(
        task_ids='move_file_with_timestamp',
        key='latest_filename'
    )
    
    print("\n" + "=" * 80)
    print("  ⚙️ SPARK JOB SUBMISSION")
    print("=" * 80)
    print(f"  🔧 Job Name: AmountDataProcessor")
    print(f"  📝 Application: s3_amount_processor.py")
    print(f"  📄 Processing File: {latest_file}")
    print(f"  🎯 Task ID: {context['task'].task_id}")
    print(f"  📅 Execution Time: {context['execution_date']}")
    print("=" * 80 + "\n")
    
    logging.info(f"⚙️ Spark job started successfully for {latest_file}")

def log_completion(**context):
    """Log completion message"""
    task_instance = context['task_instance']
    latest_file = task_instance.xcom_pull(
        task_ids='move_file_with_timestamp',
        key='latest_filename'
    )
    
    print("\n" + "=" * 80)
    print("  ✅ PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    print(f"  📄 Processed File: {latest_file}")
    print(f"  ✨ All tasks executed successfully")
    print(f"  📊 Processed data is now available in MinIO warehouse bucket")
    print(f"\n  🔗 ACCESS RESULTS:")
    print(f"     📊 Spark History: http://localhost:18080")
    print(f"     🪣 MinIO Console: http://localhost:9001")
    print(f"        - Bucket: warehouse")
    print(f"        - Folder: processed/ (contains processed data)")
    print(f"        - Folder: reports/ (contains summary statistics)")
    print(f"\n  📈 NEXT STEPS:")
    print(f"     1. Upload another JSON file to {SOURCE_PREFIX}/ to trigger pipeline again")
    print(f"     2. Check MinIO warehouse bucket for new results")
    print(f"     3. Monitor Spark History Server for job details")
    print("=" * 80 + "\n")
    
    logging.info(f"✅ Pipeline completed - results available in MinIO for {latest_file}")

# Define the DAG
dag = DAG(
    'amount_data_s3_processing_pipeline',
    default_args=default_args,
    description='Move JSON file with timestamp, then process only latest file with Spark',
    schedule_interval='@hourly',  # Check every hour for new files
    catchup=False,
    tags=['spark', 's3', 'minio', 'amount-processing', 'file-trigger', 'timestamp'],
)

# ============================================================================
# TASK 1: Log Pipeline Start
# ============================================================================
task_log_start = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_pipeline_start,
    dag=dag,
)

# ============================================================================
# TASK 2: Monitor MinIO for NEW JSON Files (without timestamp)
# ============================================================================
task_wait_for_file = PythonSensor(
    task_id='wait_for_json_files_in_minio',
    python_callable=check_for_new_json_files,
    poke_interval=30,  # Check every 30 seconds
    timeout=3600,  # Wait up to 1 hour
    mode='poke',
    exponential_backoff=True,
    max_wait=3600,
    dag=dag,
)

# ============================================================================
# TASK 3: Move File with Timestamp
# ============================================================================
task_move_file = PythonOperator(
    task_id='move_file_with_timestamp',
    python_callable=move_file_with_timestamp,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 4: Log File Detection
# ============================================================================
task_log_detection = PythonOperator(
    task_id='log_file_detected',
    python_callable=log_file_detected,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 5: Log Spark Job Start
# ============================================================================
task_log_spark_start = PythonOperator(
    task_id='log_spark_job_start',
    python_callable=log_spark_job_started,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 6: Submit Spark Job
# ============================================================================
task_process_with_spark = SparkSubmitOperator(
    task_id='submit_spark_job',
    
    # Spark Configuration
    conn_id='spark-conn',
    application='jobs/s3_amount_processor.py',
    name='AmountDataProcessor_{{ ts_nodash }}',
    
    # Spark Submit Configuration
    conf={
        # Event Logging for History Server
        'spark.eventLog.enabled': 'true',
        'spark.eventLog.dir': '/opt/spark-events',
        'spark.history.fs.logDirectory': '/opt/spark-events',
        
        # MinIO/S3 Credentials
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'admin',
        'spark.hadoop.fs.s3a.secret.key': 'password',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        
        # Performance Settings
        'spark.sql.shuffle.partitions': '200',
        'spark.dynamicAllocation.enabled': 'false',
    },
    
    # Executor Configuration
    num_executors=2,
    executor_cores=2,
    executor_memory='2g',
    driver_memory='1g',
    
    # Job Settings
    verbose=True,
    dag=dag,
)

# ============================================================================
# TASK 7: Log Completion
# ============================================================================
task_log_completion = PythonOperator(
    task_id='log_pipeline_completion',
    python_callable=log_completion,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# SET TASK DEPENDENCIES
# ============================================================================
task_log_start >> task_wait_for_file >> task_move_file >> task_log_detection >> \
task_log_spark_start >> task_process_with_spark >> task_log_completion