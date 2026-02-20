from pyspark.sql import SparkSession
import pandas as pd
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("Retail Data Lake - Bronze Layer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

input_path = "../data/raw/superstore.csv"
bronze_output_path = "../data/bronze/bronze_data.csv"

# Read raw CSV with Spark
df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True
)

print("Schema of Raw Data:")
df.printSchema()

print("Total Rows:", df.count())

# Convert to Pandas
pdf = df.toPandas()

# Ensure bronze folder exists
os.makedirs("../data/bronze", exist_ok=True)

# Save using Pandas (bypasses Hadoop)
pdf.to_csv(bronze_output_path, index=False)

print("Bronze layer successfully created using Pandas write.")

spark.stop()