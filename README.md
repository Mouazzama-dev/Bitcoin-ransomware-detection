# 🛡️ Bitcoin Ransomware Detection System

This is a Big Data Engineering project for real-time detection of Bitcoin ransomware transactions using the **Kafka-Spark-Hadoop** stack. It addresses the challenge of high class imbalance in blockchain data using specialized ML techniques.

---

## 🏗️ Project Architecture


- **Data Ingestion**: Python-based Kafka Producer.
- **Message Broker**: Apache Kafka (KRaft Mode - Zookeeperless).
- **Stream Processing**: Apache Spark Structured Streaming.
- **Storage**: Hadoop HDFS (Data stored in Parquet format).
- **Machine Learning**: PySpark MLlib (Random Forest Classifier with Binary Balancing).

---

## 📊 Machine Learning Performance
Bitcoin transaction data is naturally imbalanced (99% safe). We implemented **Binary Classification** with **Undersampling** to ensure the model focuses on ransomware patterns.

| Metric | Result | Insight |
| :--- | :--- | :--- |
| **Model Type** | Random Forest (Balanced) | Handles non-linear features like `weight` and `looped`. |
| **Accuracy** | **68.16%** | Realistic accuracy after removing majority class bias. |
| **F1-Score** | **0.7973** | Confirms high effectiveness in detecting actual threats. |
| **Threshold** | **0.3** | Sensitivity tuned to trigger alerts at 30% certainty. |



---

## 🛠️ Key Technical Implementations
Data Balancing: Undersampled the "White" (safe) class to create a 50/50 ratio for training.

Feature Engineering: Used StandardScaler to normalize features like income and neighbors.

Custom UDF: Implemented a User Defined Function to extract probabilities from Spark ML Vectors for real-time thresholding.

## 🚀 How to Run the Pipeline

### 1. Setup Infrastructure
Start all services using Docker Compose:
```bash
sudo docker-compose up -d
```

### 2. Execute Spark Streaming Job
Submit the processing script to the Spark Master:
```bash
sudo docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/spark_jobs/ml_prediction_job.py
```

### 3. Start Data Producer
Push CSV transactions into the Kafka pipeline:
```bash
python3 producer/csv_producer.py
```

## 📂 Project Structure

```text
├── producer/              # Kafka producer script (CSV to Broker)
├── ml_jobs/               # Training & Evaluation scripts (V3 Binary)
├── spark_jobs/            # Core Streaming script (Inference engine)
├── docker-compose.yml     # Infrastructure (Spark, Kafka, Hadoop)
├── .gitignore             # Exclusion of heavy data & pycache
└── README.md              # Project documentation         
```

## Tech Stack
Spark: v3.5.0

Kafka: Latest (KRaft)

Hadoop: v3.2.1


Python: v3.10

Docker: Containerized environment