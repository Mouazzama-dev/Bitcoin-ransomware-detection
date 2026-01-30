from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("BitcoinRansomwareDetection") \
    .getOrCreate()

# Schema Definition
schema = StructType([
    StructField("address", StringType()),
    StructField("year", IntegerType()),
    StructField("day", IntegerType()),
    StructField("length", IntegerType()),
    StructField("weight", DoubleType()),
    StructField("count", IntegerType()),
    StructField("looped", IntegerType()),
    StructField("neighbors", IntegerType()),
    StructField("income", DoubleType()),
    StructField("label", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "btc-transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Process JSON
clean_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Filter Ransomware
ransomware_alerts = clean_df.filter(col("label") != "white")

# Save to Hadoop (HDFS)
hdfs_query = ransomware_alerts.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/user/mouazzama/alerts") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/mouazzama/checkpoints") \
    .start()

# Also Show on Console
console_query = ransomware_alerts.writeStream \
    .format("console") \
    .start()

spark.streams.awaitAnyTermination()