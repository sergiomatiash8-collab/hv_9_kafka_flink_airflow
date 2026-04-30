import pandas as pd
import json
import time
import os
from kafka import KafkaProducer

# 1. Define Kafka address
# localhost:9092 is used for running from PowerShell (Windows)
kafka_server = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

print(f"Connecting to Kafka at: {kafka_server}")

try:
    producer = KafkaProducer(
        bootstrap_servers=[kafka_server],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )
except Exception as e:
    print(f"Kafka connection error: {e}")
    print("Make sure the kafka container is running (docker ps)")
    exit(1)

topic_name = 'tweets'

def stream_data():
    file_path = "raw_tweets.csv"

    if not os.path.exists(file_path):
        print(f"File {file_path} not found in script directory!")
        return

    # 2. Read CSV (utf-8 encoding for Windows)
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except Exception as e:
        print(f"CSV reading error: {e}")
        return

    print(f"Starting to send {len(df)} messages to topic: {topic_name}")

    for index, row in df.iterrows():
        # Create dictionary from row data
        message = {
            "author_id": str(row['author_id']),
            "created_at": str(row['created_at']),
            "text": str(row['text'])
        }

        try:
            # 3. Send to Kafka
            producer.send(topic_name, value=message)

            if index % 10 == 0:
                print(f"Sent {index} messages...")
        except Exception as e:
            print(f"Send error at row {index}: {e}")

        time.sleep(0.5)  # Streaming simulation

    producer.flush()
    print("All data has been sent successfully!")

if __name__ == "__main__":
    stream_data()