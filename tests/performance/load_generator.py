import json
import time
import random
from kafka import KafkaProducer

# Налаштування
TOPIC = 'tweets'
BROKERS = ['localhost:9092']
TOTAL_RECORDS = 100_000 

def run_load_test():
    # Оптимізований продюсер для високого навантаження
    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Групуємо повідомлення по 64КБ або чекаємо 50мс перед відправкою пачки
        batch_size=65536,
        linger_ms=50,
        compression_type='gzip' # Стиснення економить трафік при великих обсягах
    )

    print(f"🚀 Запуск Load Test: {TOTAL_RECORDS} повідомлень...")
    print(f"📡 Топік: {TOPIC} | Брокери: {BROKERS}")
    
    start_time = time.time()

    try:
        for i in range(1, TOTAL_RECORDS + 1):
            # ВАЖЛИВО: Назви полів мають точно збігатися з CREATE TABLE у Flink
            tweet = {
                "author_id": i,  # Виправлено з 'id' на 'author_id'
                "text": f"Performance test tweet #{i} | Generate load for Flink & Postgres #{random.randint(100, 999)}",
                "created_at": time.strftime('%Y-%m-%d %H:%M:%S'), # Формат, який легше перетравить БД
                "user_name": f"bot_tester_{random.randint(1, 500)}"
            }
            
            producer.send(TOPIC, tweet)

            if i % 20000 == 0:
                print(f"📈 Прогрес: {i} відправлено...")

        print("⏳ Завершуємо передачу залишків (flushing)...")
        producer.flush()
        
        end_time = time.time()
        duration = end_time - start_time
        
        print("\n" + "="*30)
        print(f"🏁 ТЕСТ ЗАВЕРШЕНО")
        print(f"⏱ Час виконання: {duration:.2f} сек")
        print(f"📊 Швидкість: {int(TOTAL_RECORDS / duration)} msg/sec")
        print("="*30)

    except Exception as e:
        print(f"❌ Помилка під час генерації: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    run_load_test()