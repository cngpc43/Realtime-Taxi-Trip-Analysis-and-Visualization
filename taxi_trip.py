from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, from_json, stddev, max as spark_max, min as spark_min

# Initialize Spark Session without specifying spark.jars
spark = SparkSession.builder \
    .appName("PulsarSparkAnalytics") \
    .getOrCreate()

# Read from Pulsar using readStream for streaming DataFrame
df = spark.readStream \
    .format("pulsar") \
    .option("service.url", "pulsar://localhost:6650") \
    .option("admin.url", "http://localhost:8080") \
    .option("topic", "persistent://public/default/trip-topic") \
    .load()

# Verify if DataFrame is streaming
if not df.isStreaming:
    raise ValueError("The DataFrame is not a streaming DataFrame. Ensure you are using readStream.")

# Parse JSON data using the 'value' column
trip_df = df.selectExpr("cast(value as string) as json") \
    .select(from_json(col("json"), "struct<trip_duration:double>").alias("trip"))

# Perform Analytics
analytics_df = trip_df.select("trip.*") \
    .agg(
        avg("trip_duration").alias("average_duration"),
        stddev("trip_duration").alias("stddev_duration"),
        spark_max("trip_duration").alias("max_duration"),
        spark_min("trip_duration").alias("min_duration"),
        count("*").alias("total_trips")
    )

# Write the analytics results to the console in streaming mode
query_analytics = analytics_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Await termination of the streaming query
spark.streams.awaitAnyTermination()