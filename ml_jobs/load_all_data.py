from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("BulkDataLoader").getOrCreate()

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

csv_path = "/opt/spark/data/BitcoinHeistData.csv"
df = spark.read.csv(csv_path, header=True, schema=schema)

df.write.mode("overwrite").parquet("hdfs://namenode:9000/user/mouazzama/all_transactions")

print("SUCCESS: Full dataset uploaded to HDFS for training!")
spark.stop()