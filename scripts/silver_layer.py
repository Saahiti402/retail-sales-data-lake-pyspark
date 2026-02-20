from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, expr
import pandas as pd
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("Retail Data Lake - Silver Layer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

bronze_input_path = "../data/bronze/bronze_data.csv"
silver_output_path = "../data/silver/silver_data.csv"

# Read Bronze Data
df = spark.read.csv(
    bronze_input_path,
    header=True,
    inferSchema=True,
    multiLine=True,
    quote='"',
    escape='"'
)

print("Initial Row Count:", df.count())

# Remove Duplicates
df = df.dropDuplicates()
print("After Deduplication:", df.count())

# Cleaning Sales Column PROPERLY
# Remove everything except digits and decimal
df = df.withColumn(
    "Sales",
    regexp_replace(col("Sales"), "[^0-9.]", "")
)

# Use Spark SQL safe cast
df = df.withColumn(
    "Sales",
    expr("try_cast(Sales as double)")
)

# Convert Date Columns
df = df.withColumn(
    "Order Date",
    to_date(col("Order Date"), "dd/MM/yyyy")
)

df = df.withColumn(
    "Ship Date",
    to_date(col("Ship Date"), "dd/MM/yyyy")
)

# Handle Null Values
df = df.na.fill({
    "Postal Code": 0,
    "Sales": 0.0
})

# Standardize Column Names
for column in df.columns:
    new_col = column.strip().lower().replace(" ", "_").replace("-", "_")
    df = df.withColumnRenamed(column, new_col)

print("Final Schema:")
df.printSchema()

print("Final Row Count:", df.count())

# Save Using Pandas (No Hadoop dependency)
pdf = df.toPandas()

os.makedirs("../data/silver", exist_ok=True)
pdf.to_csv(silver_output_path, index=False)

print("Silver layer successfully created.")

spark.stop()
