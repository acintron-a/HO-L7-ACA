from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder \
    .appName("DriverAggregations") \
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
parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
watermarked_df = parsed_df.withWatermark("timestamp", "1 minute")

# Compute aggregations: total fare and average distance grouped by driver_id
grouped_df = watermarked_df.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Define a function to write each batch to a CSV file
def write_batch_to_csv(batch_df, batch_id):
    # Print to console for visibility
    print(f"-------------------------------------------")
    print(f"Batch: {batch_id}")
    print(f"-------------------------------------------")
    batch_df.show(truncate=False)
    
    # Save the batch DataFrame as a CSV file with the batch ID in the filename
    batch_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"outputs/task_2/batch_{batch_id}")

# Use foreachBatch to apply the function to each micro-batch
query = grouped_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch_to_csv) \
    .start()

query.awaitTermination()
