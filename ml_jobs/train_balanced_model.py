from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("BalancedRansomwareModel").getOrCreate()

# 1. Load data from HDFS
data = spark.read.parquet("hdfs://namenode:9000/user/mouazzama/all_transactions")

# 2. Features selection (exclude address and label)
feature_cols = ["year", "day", "length", "weight", "count", "looped", "neighbors", "income"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# 3. Convert Label (string) to Index (numeric)
label_indexer = StringIndexer(inputCol="label", outputCol="label_index")

# 4. Train/Test Split (80% for training, 20% for testing)
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# 5. Define Random Forest Classifier
rf = RandomForestClassifier(featuresCol="features", labelCol="label_index", numTrees=20)

# 6. Create Pipeline and Train
pipeline = Pipeline(stages=[assembler, label_indexer, rf])
model = pipeline.fit(train_data)

# 7. Evaluate the Model
predictions = model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print(f"\n" + "="*30)
print(f"MODEL ACCURACY: {accuracy * 100:.2f}%")
print("="*30 + "\n")

# 8. Save the Model to HDFS
model.write().overwrite().save("hdfs://namenode:9000/user/mouazzama/models/rf_balanced_model")

print("SUCCESS: Balanced model saved to HDFS!")
spark.stop()