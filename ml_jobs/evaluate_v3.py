from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("ModelEvaluationV3").getOrCreate()

# 1. Load Data aur Model
data = spark.read.parquet("hdfs://namenode:9000/user/mouazzama/all_transactions")
model = PipelineModel.load("hdfs://namenode:9000/user/mouazzama/models/rf_binary_balanced")

# 2. Binary Label data
test_data = data.withColumn("binary_label", when(col("label") == "white", 0.0).otherwise(1.0))

# 3. Predictions 
predictions = model.transform(test_data)

# 4. Evaluators 
evaluator_acc = MulticlassClassificationEvaluator(labelCol="binary_label", predictionCol="prediction", metricName="accuracy")
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="binary_label", predictionCol="prediction", metricName="f1")

# 5. Metrics 
accuracy = evaluator_acc.evaluate(predictions)
f1_score = evaluator_f1.evaluate(predictions)

print("\n" + "="*50)
print(f"MODEL V3 PERFORMANCE REPORT")
print(f"Overall Accuracy: {accuracy * 100:.2f}%")
print(f"F1-Score (Balanced Performance): {f1_score:.4f}")
print("="*50 + "\n")

spark.stop()