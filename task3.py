# import the necessary libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder \
    .appName("WindowedAnalytics") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

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

# Convert timestamp column to TimestampType and add a watermark
parsed_df = parsed_df.withColumn("event_time", col("timestamp").cast(TimestampType()))
watermarked_df = parsed_df.withWatermark("event_time", "1 minute")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_df = watermarked_df.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).agg(
    sum("fare_amount").alias("total_fare")
)

# Extract window start and end times as separate columns
extracted_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# Define a function to write each batch to a CSV file with column names
def write_batch_to_csv(batch_df, batch_id):
    # Print to console for visibility
    print(f"-------------------------------------------")
    print(f"Batch: {batch_id}")
    print(f"-------------------------------------------")
    batch_df.show(truncate=False)
    
    # Save the batch DataFrame as a CSV file with headers included
    batch_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"outputs/task_3/batch_{batch_id}")
    
# Use foreachBatch to apply the function to each micro-batch
query = extracted_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_batch_to_csv) \
    .start()

query.awaitTermination()
