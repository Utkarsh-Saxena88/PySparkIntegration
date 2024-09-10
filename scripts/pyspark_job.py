# pyspark_job.py
from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Simple PySpark Job") \
        .getOrCreate()

    # Example: Create a DataFrame
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    
    # Show DataFrame
    df.show()

    # Perform some operation (like filtering data)
    filtered_df = df.filter(df.Age > 30)
    filtered_df.show()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
