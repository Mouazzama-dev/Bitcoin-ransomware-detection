from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType
from pyspark.ml import PipelineModel

# 1. Spark Session
spark = SparkSession.builder.appName("LiveRansomwareDetection").getOrCreate()

# 2. Schema Define karein
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

# 3. Model Load (Binary Balanced Version)
model = PipelineModel.load("hdfs://namenode:9000/user/mouazzama/models/rf_binary_balanced")

# 4. UDF define karein probability vector se Ransomware prob (index 1) nikalne ke liye
# Spark ML ka probability column ek Vector hota hai, isliye UDF zaroori hai
extract_prob_udf = udf(lambda v: float(v[1]), FloatType())

# 5. Kafka Stream Connect karein
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "btc-transactions") \
    .load()

# 6. JSON Parse karein
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 7. Model Transformation
predictions = model.transform(parsed_df)

# 8. Threshold Logic apply karein (0.3 sensitivity)
# Hum 0.5 ke bajaye 0.3 use kar rahe hain taake detection behtar ho
final_view = predictions.withColumn("ransom_prob", extract_prob_udf(col("probability"))) \
    .select(
        "address", 
        "label",
        when(col("ransom_prob") > 0.3, "⚠️ RANSOMWARE").otherwise("✅ SAFE").alias("Status"),
        col("ransom_prob").alias("Certainty")
    )

# 9. Output to Console
query = final_view.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()