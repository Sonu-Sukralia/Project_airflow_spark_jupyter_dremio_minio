# from pyspark.sql import SparkSession
# from datetime import datetime

# def main():
#     # Create Spark session with proper logging configuration
#     spark = SparkSession.builder \
#         .appName(f"Airflow_Job_{datetime.now().strftime('%Y%m%d_%H%M%S')}") \
#         .master("spark://spark-master:7077") \
#         .config("spark.eventLog.enabled", "true") \
#         .config("spark.eventLog.dir", "/opt/spark-events") \
#         .config("spark.history.fs.logDirectory", "/opt/spark-events") \
#         .getOrCreate()
    
#     print("=" * 60)
#     print("🚀 Airflow-triggered Spark Job Started")
#     print("=" * 60)
    
#     # Create sample data
#     data = [
#         (1, "Alice", 25, "Engineering"),
#         (2, "Bob", 30, "Sales"),
#         (3, "Charlie", 35, "Engineering"),
#         (4, "Diana", 28, "Marketing"),
#     ]
    
#     df = spark.createDataFrame(data, ["id", "name", "age", "department"])
    
#     print("\n📊 Sample Data:")
#     df.show()
    
#     # Perform aggregations
#     print("\n📈 Department Statistics:")
#     dept_stats = df.groupBy("department").count()
#     dept_stats.show()
    
#     # Write to MinIO
#     output_path = f"s3a://test/airflow_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
#     print(f"\n💾 Writing results to: {output_path}")
#     dept_stats.write.mode("overwrite").parquet(output_path)
    
#     print("\n" + "=" * 60)
#     print("✅ Job Completed Successfully")
#     print("=" * 60)
    
#     spark.stop()

# if __name__ == "__main__":
#     main()





# from pyspark.sql import SparkSession
# from datetime import datetime

# def main():
#     # Don't set master or event logging configs here
#     # SparkSubmitOperator already handles these
#     spark = SparkSession.builder \
#         .appName(f"Airflow_Job_{datetime.now().strftime('%Y%m%d_%H%M%S')}") \
#         .getOrCreate()
    
#     print("=" * 60)
#     print("🚀 Airflow-triggered Spark Job Started")
#     print("=" * 60)
    
#     # Create sample data
#     data = [
#         (1, "Alice", 25, "Engineering"),
#         (2, "Bob", 30, "Sales"),
#         (3, "Charlie", 35, "Engineering"),
#         (4, "Diana", 28, "Marketing"),
#     ]
    
#     df = spark.createDataFrame(data, ["id", "name", "age", "department"])
    
#     print("\n📊 Sample Data:")
#     df.show()
    
#     # Perform aggregations
#     print("\n📈 Department Statistics:")
#     dept_stats = df.groupBy("department").count()
#     dept_stats.show()
    
#     # Write to MinIO
#     output_path = f"s3a://test/airflow_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
#     print(f"\n💾 Writing results to: {output_path}")
#     dept_stats.write.mode("overwrite").parquet(output_path)
    
#     print("\n" + "=" * 60)
#     print("✅ Job Completed Successfully")
#     print("=" * 60)
    
#     spark.stop()

# if __name__ == "__main__":
#     main()




from pyspark.sql import SparkSession
from datetime import datetime

def main():
    # Create Spark session
    # Don't set master or configs - SparkSubmitOperator handles these
    spark = SparkSession.builder \
        .appName(f"Airflow_Job_{datetime.now().strftime('%Y%m%d_%H%M%S')}") \
        .getOrCreate()
    
    print("=" * 60)
    print("🚀 Airflow-triggered Spark Job Started")
    print("=" * 60)
    
    # Create sample data
    data = [
        (1, "Alice", 25, "Engineering"),
        (2, "Bob", 30, "Sales"),
        (3, "Charlie", 35, "Engineering"),
        (4, "Diana", 28, "Marketing"),
    ]
    
    df = spark.createDataFrame(data, ["id", "name", "age", "department"])
    
    print("\n📊 Sample Data:")
    df.show()
    
    # Perform aggregations
    print("\n📈 Department Statistics:")
    dept_stats = df.groupBy("department").count()
    dept_stats.show()
    
    # Write to MinIO
    output_path = f"s3a://test/airflow_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"\n💾 Writing results to: {output_path}")
    dept_stats.write.mode("overwrite").parquet(output_path)
    
    print("\n" + "=" * 60)
    print("✅ Job Completed Successfully")
    print("📊 Check History Server: http://localhost:18080")
    print("💾 Check MinIO Console: http://localhost:9001")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()