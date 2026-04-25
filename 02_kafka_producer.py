import pandas as pd
import json
import time
import os
from kafka import KafkaProducer

# 1. Визначаємо адресу Kafka
# Для запуску з PowerShell (Windows) використовується localhost:9092
kafka_server = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

print(f"🔌 Підключення до Kafka за адресою: {kafka_server}")

try:
    producer = KafkaProducer(
        bootstrap_servers=[kafka_server],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )
except Exception as e:
    print(f"❌ Помилка підключення до Kafka: {e}")
    print("Переконайся, що контейнер kafka запущений (docker ps)")
    exit(1)

topic_name = 'tweets'

def stream_data():
    file_path = "raw_tweets.csv"
    
    if not os.path.exists(file_path):
        print(f"❌ Файл {file_path} не знайдено у папці зі скриптом!")
        return

    # 2. Читаємо CSV (з кодуванням utf-8 для Windows)
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except Exception as e:
        print(f"❌ Помилка читання CSV: {e}")
        return

    print(f"📡 Починаємо відправку {len(df)} повідомлень у топік: {topic_name}")

    for index, row in df.iterrows():
        # Створюємо словник з даних рядка
        message = {
            "author_id": str(row['author_id']),
            "created_at": str(row['created_at']),
            "text": str(row['text'])
        }
        
        try:
            # 3. Відправка в Kafka
            producer.send(topic_name, value=message)
            
            if index % 10 == 0:
                print(f"✅ Відправлено {index} повідомлень...")
        except Exception as e:
            print(f"⚠️ Помилка відправки на рядку {index}: {e}")
            
        time.sleep(0.5) # Імітація стримінгу

    producer.flush()
    print("🏁 Відправку всіх даних завершено!")

if __name__ == "__main__":
    stream_data()