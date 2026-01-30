# 🛡️ Bitcoin Ransomware Detection System

This is a Big Data Engineering project for real-time detection of Bitcoin ransomware transactions using the **Kafka-Spark-Hadoop** stack.

---

## 🏗️ Project Architecture
- **Data Ingestion**: Python-based Kafka Producer.
- **Message Broker**: Apache Kafka (KRaft Mode - Zookeeperless).
- **Stream Processing**: Apache Spark Structured Streaming.
- **Storage**: Hadoop HDFS (Data stored in Parquet format).
- **Machine Learning**: PySpark MLlib (Random Forest Classifier).

---

## 🚀 How to Run the Pipeline

### 1. Setup Infrastructure
Start all services using Docker Compose:
$ sudo docker-compose up -d

### 2. Execute Spark Streaming Job
Submit the processing script to the Spark Master:
$ sudo docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/spark_jobs/ransomware_detector.py

### 3. Start Data Producer
Push CSV transactions into the Kafka pipeline from your local environment:
$ source btc_env/bin/activate
$ python3 producer/csv_producer.py

---

## 📁 HDFS Management
Verify your data is being saved correctly in Hadoop:

| Goal | Command |
| :--- | :--- |
| **List Alerts** | sudo docker exec -it namenode hdfs dfs -ls /user/mouazzama/alerts |
| **Check Models** | sudo docker exec -it namenode hdfs dfs -ls /user/mouazzama/models |

---

## 🛠️ Tech Stack
- **Spark**: v3.5.0
- **Kafka**: Latest (KRaft)
- **Hadoop**: v3.2.1
- **Python**: v3.10
- **Docker**: Containerized environment
