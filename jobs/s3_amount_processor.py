
#!/usr/bin/env python3
"""
Spark Job: Amount Data Processor
Purpose: Read JSON files with id and amount from MinIO source bucket,
process them, and write results to target bucket
JSON Format: {"id": 1, "amount": 1000}
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, max, min, count,
    lit, current_timestamp, round as spark_round, when
)
from datetime import datetime
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_section(title):
    """Print formatted section title"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")

def create_spark_session():
    """Create and configure Spark session with MinIO support"""
    log_section("🚀 INITIALIZING SPARK SESSION")
#       .config("spark.eventLog.dir", "/opt/spark-events") \
    spark = SparkSession.builder \
        .appName("AmountDataProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.261") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:///opt/spark-events") \
        .config("spark.history.fs.logDirectory", "file:///opt/spark-events") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("✅ Spark Session Created Successfully!")
    print(f"   Spark Version: {spark.version}")
    print(f"   App Name: {spark.sparkContext.appName}")
    print(f"   Master: {spark.sparkContext.master}")
    print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    return spark

def read_json_from_minio(spark, bucket_name):
    """Read JSON files from MinIO S3 bucket"""
    log_section("📂 READING JSON FILES FROM MinIO")

    try:
        # S3A path for MinIO
        s3_path = f"s3a://{bucket_name}/source/"

        print(f"📍 Source Path: {s3_path}")
        print(f"🪣 Bucket: {bucket_name}\n")

        # Read JSON files
        df = spark.read \
            .format("json") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt") \
            .load(s3_path)

        row_count = df.count()

        print(f"✅ Successfully read JSON files!")
        print(f"   Total Records: {row_count:,}")
        print(f"   Total Columns: {len(df.columns)}")
        print(f"   Column Names: {', '.join(df.columns)}\n")

        # Display sample data
        print("📋 Sample Data (First 5 rows):")
        df.show(n=5, truncate=False)

        return df

    except Exception as e:
        logger.error(f"❌ Error reading JSON files: {str(e)}")
        print(f"\n❌ ERROR: Failed to read JSON files")
        print(f"   Reason: {str(e)}")
        raise

def process_and_enrich_data(df):
    """Process and enrich the data with calculations"""
    log_section("⚙️ PROCESSING DATA")

    try:
        # Add processing metadata
        df_processed = df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("processing_date", lit(datetime.now().strftime("%Y-%m-%d"))) \
            .withColumn("processing_time", lit(datetime.now().strftime("%H:%M:%S"))) \
            .withColumn("status", lit("PROCESSED")) \
            .withColumn("amount_category",
                        when(col("amount") < 2000, "LOW").otherwise("HIGH"))

        print(f"✅ Data enrichment completed!")
        print(f"   Added Columns: processing_timestamp, processing_date, processing_time, status, amount_category")
        print(f"   Total Records: {df_processed.count():,}\n")

        # Display processed data
        print("📊 Processed Data Sample:")
        df_processed.show(n=5, truncate=False)

        return df_processed

    except Exception as e:
        logger.error(f"❌ Error processing data: {str(e)}")
        print(f"\n❌ ERROR: Failed to process data")
        print(f"   Reason: {str(e)}")
        raise

def calculate_statistics(df):
    """Calculate and display statistics"""
    log_section("📊 CALCULATING STATISTICS")

    try:
        # Calculate statistics
        stats_df = df.agg(
            count("id").alias("total_records"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("average_amount"),
            max("amount").alias("max_amount"),
            min("amount").alias("min_amount")
        )

        stats = stats_df.collect()[0].asDict()

        print(f"📈 Amount Statistics:")
        print(f"   Total Records: {int(stats['total_records']):,}")
        print(f"   Total Amount: ${int(stats['total_amount']):,}")
        print(f"   Average Amount: ${stats['average_amount']:,.2f}")
        print(f"   Maximum Amount: ${int(stats['max_amount']):,}")
        print(f"   Minimum Amount: ${int(stats['min_amount']):,}\n")

        return stats

    except Exception as e:
        logger.warning(f"⚠️  Could not calculate statistics: {str(e)}")
        return None

def write_to_minio(df, bucket_name):
    """Write processed data to MinIO S3 bucket"""
    log_section("💾 WRITING PROCESSED DATA TO MinIO")

    try:
        # S3A path for MinIO target
        s3_target_path = f"s3a://{bucket_name}/processed/"

        print(f"📍 Target Path: {s3_target_path}")
        print(f"🪣 Target Bucket: {bucket_name}\n")

        # Write in Parquet format
        df.coalesce(1) \
            .write \
            .format("parquet") \
            .mode("overwrite") \
            .save(s3_target_path)

        print(f"✅ Successfully wrote data to MinIO!")
        print(f"   Format: Parquet (compressed)")
        print(f"   Records Written: {df.count():,}")
        print(f"   Target Location: {s3_target_path}\n")

    except Exception as e:
        logger.error(f"❌ Error writing to MinIO: {str(e)}")
        print(f"\n❌ ERROR: Failed to write data")
        print(f"   Reason: {str(e)}")
        raise

def write_summary_to_minio(df, bucket_name):
    """Write summary statistics to MinIO"""
    log_section("📄 WRITING SUMMARY REPORT")

    try:
        # Calculate summary stats
        summary_df = df.agg(
            count("id").alias("total_records"),
            sum("amount").alias("total_amount"),
            spark_round(avg("amount"), 2).alias("average_amount"),
            max("amount").alias("max_amount"),
            min("amount").alias("min_amount")
        ) \
        .withColumn("report_date", lit(datetime.now().strftime("%Y-%m-%d"))) \
        .withColumn("report_time", lit(datetime.now().strftime("%H:%M:%S")))

        # Write summary
        summary_path = f"s3a://{bucket_name}/reports/"
        summary_df.coalesce(1) \
            .write \
            .format("parquet") \
            .mode("overwrite") \
            .save(summary_path)

        print(f"✅ Summary report written!")
        print(f"   Location: {summary_path}")
        print(f"   Format: Parquet\n")

    except Exception as e:
        logger.warning(f"⚠️  Could not write summary: {str(e)}")

def main():
    """Main execution function"""
    log_section("🎯 AMOUNT DATA PROCESSOR - STARTING PIPELINE")

    start_time = datetime.now()

    try:
        # Configuration
        SOURCE_BUCKET = "test"  # MinIO bucket for source files
        TARGET_BUCKET = "warehouse"  # MinIO bucket for processed files

        print("⚙️  Configuration:")
        print(f"   Source Bucket: {SOURCE_BUCKET}")
        print(f"   Target Bucket: {TARGET_BUCKET}")
        print(f"   Source Path: s3a://{SOURCE_BUCKET}/source/")
        print(f"   Target Path: s3a://{TARGET_BUCKET}/processed/")
        print(f"   MinIO Endpoint: http://minio:9000")
        print(f"   Spark Master: spark://spark-master:7077\n")

        # Step 1: Create Spark Session
        spark = create_spark_session()

        # Step 2: Read JSON from MinIO
        df_raw = read_json_from_minio(spark, SOURCE_BUCKET)

        # Step 3: Process and enrich data
        df_processed = process_and_enrich_data(df_raw)

        # Step 4: Calculate statistics
        stats = calculate_statistics(df_raw)

        # Step 5: Write processed data to MinIO
        write_to_minio(df_processed, TARGET_BUCKET)

        # Step 6: Write summary report
        write_summary_to_minio(df_raw, TARGET_BUCKET)

        # Success summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        log_section("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print("📊 FINAL SUMMARY:")
        print(f"   ✅ Read JSON files from MinIO source bucket")
        print(f"   ✅ Enriched data with metadata columns")
        print(f"   ✅ Calculated statistics")
        print(f"   ✅ Wrote processed data to target bucket")
        print(f"   ✅ Generated summary report")
        print(f"\n⏱️  Processing Time: {duration:.2f} seconds")
        print(f"\n🔗 ACCESS POINTS:")
        print(f"   📊 Spark History Server: http://localhost:18080")
        print(f"   🪣 MinIO Console: http://localhost:9001")
        print(f"      - Check 'warehouse' bucket")
        print(f"      - Look for 'processed/' and 'reports/' folders")
        print(f"\n")

        spark.stop()

    except Exception as e:
        log_section("❌ PIPELINE FAILED!")
        logger.error(f"Fatal Error: {str(e)}")
        print(f"❌ Error Details: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()