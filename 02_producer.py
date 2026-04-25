import pandas as pd
import json
import time
from kafka import KafkaProducer

# 1. Ініціалізація продюсера
# bootstrap_servers='localhost:9092' — це порт, який ми відкрили в docker-compose
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'tweets'

def stream_data():
    # 2. Читаємо наш локальний CSV файл
    df = pd.read_csv("raw_tweets.csv")
    
    print(f"📡 Починаємо відправку повідомлень у топік: {topic_name}")

    for index, row in df.iterrows():
        # Формуємо словник (JSON-повідомлення)
        message = {
            "author_id": str(row['author_id']),
            "created_at": str(row['created_at']),
            "text": str(row['text'])
        }
        
        # 3. Відправляємо в Kafka
        producer.send(topic_name, value=message)
        
        if index % 10 == 0:
            print(f"✅ Відправлено {index} повідомлень...")
            
        time.sleep(0.5) # Невелика затримка для імітації потоку

    producer.flush() # Дочекатися завершення відправки

if __name__ == "__main__":
    stream_data()