from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into columns using the defined schema
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Print parsed data to the CSV files and console
def write_to_csv_and_console(batch_df, batch_id):
    # Print to console
    print(f"-------------------------------------------")
    print(f"Batch: {batch_id}")
    print(f"-------------------------------------------")
    batch_df.show(truncate=False)
    
    # Save to CSV
    batch_df.coalesce(1).write \
        .mode("append") \
        .option("header", "true") \
        .csv("outputs/task_1")

query = parsed_df.writeStream \
    .foreachBatch(write_to_csv_and_console) \
    .start()

query.awaitTermination()
