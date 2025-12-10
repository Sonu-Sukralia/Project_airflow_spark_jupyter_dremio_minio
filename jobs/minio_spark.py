try:
    import sys, random, uuid
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession

    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *

    from pyspark.sql.types import *
    from datetime import datetime, date
    import boto3
    from functools import reduce
    from pyspark.sql import Row
    from faker import Faker
except Exception as e:
    print("Modules are missing : {} ".format(e))

job_start_ts = datetime.now()
ts_format = '%Y-%m-%d %H:%M:%S'
minio_endpoint = 'http://minio:9000/'
minio_access_key = 'admin'
minio_secret_key = 'password'
minio_secure = False


spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_endpoint)
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")





# -----------------------------
# Sample DataFrame
# -----------------------------
data = [("Sonusds", 30), ("Aliceaaa", 25), ("Bobsdsd", 40)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# -----------------------------
# Write to MinIO bucket 'test'
# -----------------------------
output_path = "s3a://test/outputv2_data"
df.write.format("csv").mode("overwrite").option("header", "true").save(output_path)

print(f"Data written successfully to {output_path}")

# Stop Spark
spark.stop()


