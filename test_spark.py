from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("IMDB Dataset EDA") \
    .getOrCreate()

# HDFS path to your file
hdfs_path = "hdfs://localhost:9000/test/data/IMDB-Dataset.csv"

# Read CSV from HDFS
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Show DataFrame schema
print("=== Schema ===")
df.printSchema()

# Show first 5 rows
print("=== First 5 Rows ===")
df.show(5)

# Show basic descriptive statistics
print("=== Describe (Stats) ===")
df.describe().show()

# Show column names
print("=== Columns ===")
print(df.columns)

# Example: Count total rows
print("=== Total rows ===")
print(df.count())

# Stop the Spark session
spark.stop()
