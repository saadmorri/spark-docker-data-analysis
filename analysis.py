from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, to_timestamp, month, hour

# Start Spark
spark = SparkSession.builder.appName("SimpleAnalysis").getOrCreate()
import time
# Read CSV
df = spark.read.csv("/data/311.csv", header=True, inferSchema=True)

# Print schema (requirement)
print("=== SCHEMA ===")
df.printSchema()

# Convert date
df = df.withColumn(
    "created_ts",
    to_timestamp(col("Created Date"), "MM/dd/yyyy hh:mm:ss a")
)

df = df.withColumn("month", month("created_ts"))
df = df.withColumn("hour", hour("created_ts"))

# ---------------------------
# Q1: Top complaint types
# ---------------------------
print("Q1: Top complaint types")
df.groupBy("Complaint Type").count().orderBy(desc("count")).show(10)

# ---------------------------
# Q2: Top agencies
# ---------------------------
print("Q2: Top agencies")
df.groupBy("Agency").count().orderBy(desc("count")).show(10)

# ---------------------------
# Q3: Requests per month
# ---------------------------
print("Q3: Requests per month")
df.groupBy("month").count().orderBy("month").show()

# ---------------------------
# Q4: Requests per borough
# ---------------------------
print("Q4: Requests per borough")
df.groupBy("Borough").count().orderBy(desc("count")).show()

# ---------------------------
# Q5: Requests by hour
# ---------------------------
print("Q5: Requests by hour")
df.groupBy("hour").count().orderBy("count").show()

# Save Parquet
df.write.mode("overwrite").parquet("/data/output_parquet")

print("Saved Parquet file")

time.sleep(20)

spark.stop()
