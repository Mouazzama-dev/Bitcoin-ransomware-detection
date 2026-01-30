import pandas as pd
from kafka import KafkaProducer
import json
import time

# Kafka Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Dataset Path
csv_file = "datasets/BitcoinHeistData.csv"

print("Streaming started... Press Ctrl+C to stop.")

# Read CSV and send to Kafka
for chunk in pd.read_csv(csv_file, chunksize=10):
    for index, row in chunk.iterrows():
        data = row.to_dict()
        producer.send('btc-transactions', value=data)
    print(f"Sent {len(chunk)} rows to Kafka")
    time.sleep(1) # Real-time simulation