from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("BinaryBalancedModel").getOrCreate()

# 1. Load Data from HDFS
data = spark.read.parquet("hdfs://namenode:9000/user/mouazzama/all_transactions")

# 2. Binary Labeling (White = 0, Ransomware = 1)
binary_data = data.withColumn("binary_label", when(col("label") == "white", 0).otherwise(1))

# 3. Strict Balancing (50/50 ratio)
ransom_df = binary_data.filter(col("binary_label") == 1)
white_df = binary_data.filter(col("binary_label") == 0)

balanced_white = white_df.sample(False, (ransom_df.count() / white_df.count()))
final_df = ransom_df.union(balanced_white)

print(f"Dataset Balanced: Ransomware={ransom_df.count()}, White={balanced_white.count()}")

# 4. Pipeline with Scaling
feature_cols = ["year", "day", "length", "weight", "count", "looped", "neighbors", "income"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features") # Like your reference code

# Random Forest with more trees for better recall
rf = RandomForestClassifier(featuresCol="features", labelCol="binary_label", numTrees=100)

pipeline = Pipeline(stages=[assembler, scaler, rf])
model = pipeline.fit(final_df)

# 5. Save the New Model
model.write().overwrite().save("hdfs://namenode:9000/user/mouazzama/models/rf_binary_balanced")

print("SUCCESS: Binary Balanced Model V3 saved!")
spark.stop()