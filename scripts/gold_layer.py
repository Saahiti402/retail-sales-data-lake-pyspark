from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, month, year, col, expr
import pandas as pd
import os

# -----------------------------------
# Initialize Spark
# -----------------------------------
spark = SparkSession.builder \
    .appName("Retail Data Lake - Gold Layer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

silver_input_path = "../data/silver/silver_data.csv"
gold_output_path = "../data/gold"

# -----------------------------------
# Read Silver Data
# -----------------------------------
df = spark.read.csv(
    silver_input_path,
    header=True,
    inferSchema=True
)

print("Silver Row Count:", df.count())

# -----------------------------------
# Defensive Casting (Enterprise Practice)
# -----------------------------------
df = df.withColumn(
    "sales",
    expr("try_cast(sales as double)")
)

df = df.na.fill({
    "sales": 0.0
})

# -----------------------------------
# 1️⃣ Revenue by Region
# -----------------------------------
region_revenue = df.groupBy("region") \
    .agg(_sum("sales").alias("total_sales"))

# -----------------------------------
# 2️⃣ Revenue by Category
# -----------------------------------
category_revenue = df.groupBy("category") \
    .agg(_sum("sales").alias("total_sales"))

# -----------------------------------
# 3️⃣ Monthly Sales Trend
# -----------------------------------
df = df.withColumn("order_month", month("order_date")) \
       .withColumn("order_year", year("order_date"))

monthly_sales = df.groupBy("order_year", "order_month") \
    .agg(_sum("sales").alias("total_sales")) \
    .orderBy("order_year", "order_month")

# -----------------------------------
# Save Gold Outputs
# -----------------------------------
os.makedirs(gold_output_path, exist_ok=True)

region_revenue.toPandas().to_csv(
    f"{gold_output_path}/region_revenue.csv", index=False)

category_revenue.toPandas().to_csv(
    f"{gold_output_path}/category_revenue.csv", index=False)

monthly_sales.toPandas().to_csv(
    f"{gold_output_path}/monthly_sales.csv", index=False)

print("Gold layer successfully created.")

spark.stop()