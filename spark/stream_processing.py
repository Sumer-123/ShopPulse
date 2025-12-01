from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# 1. Initialize Spark Session
print("Starting Spark Session...")
spark = SparkSession.builder \
    .appName("ShopPulseStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define Schema
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("device_type", StringType(), True),
    StructField("location", StringType(), True)
])

# 3. Read from Kafka
print("Connecting to Kafka...")
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "shop_pulse_events") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parse JSON Data
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Aggregation: Count events by Type
# agg_stream = parsed_stream.groupBy("event_type").count()

# ... (Keep imports and sections 1-4 the same) ...

# 5. Add a simple transformation (e.g., extract year/month for partitioning if needed, or just keep raw columns)
# We will just write the parsed stream directly to the "Silver" layer
# This mimics a Data Lake storage pattern

# 6. Write to Parquet (The Data Lake)
print("Writing to Parquet...")
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "spark_data/silver_layer") \
    .option("checkpointLocation", "spark_data/checkpoints/silver_layer") \
    .start()

print("Streaming to Data Lake Started...")
query.awaitTermination()