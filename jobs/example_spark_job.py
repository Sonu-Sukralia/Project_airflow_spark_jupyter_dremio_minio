from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ExampleJob") \
        .getOrCreate()
    
    # Create sample data
    data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
    df = spark.createDataFrame(data, ["id", "name", "age"])
    
    print("Sample DataFrame:")
    df.show()
    
    # Perform some operations
    avg_age = df.groupBy().avg("age").collect()[0][0]
    print(f"Average age: {avg_age}")
    
    spark.stop()

if __name__ == "__main__":
    main()
