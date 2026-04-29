"""
Kafka Producer - точка входу для Producer контейнера.

Відповідальності:
    1. Читання CSV файлу з RAW даними
    2. Відправка в топік 'tweets' (без збагачення)
    3. Симуляція реального потоку даних з контрольованою швидкістю

Clean Architecture шар: Presentation Layer
"""

import pandas as pd
import json
import time
import os
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== КОНФІГУРАЦІЯ ====================

# Environment variables з fallback значеннями
KAFKA_SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'tweets')
CSV_FILE = os.environ.get('CSV_FILE', '/app/raw_tweets.csv')
MESSAGES_PER_SECOND = int(os.environ.get('MESSAGES_PER_SECOND', '2'))

# ==================== PRODUCER CREATION ====================

def create_producer(max_retries: int = 10) -> KafkaProducer:
    """
    Створення Kafka Producer з retry логікою.
    
    Args:
        max_retries: Максимальна кількість спроб підключення
        
    Returns:
        Налаштований KafkaProducer
        
    Raises:
        Exception: якщо не вдалося підключитися після всіх спроб
    """
    retry_count = 0
    retry_delay = 5  # секунд між спробами
    
    while retry_count < max_retries:
        try:
            logger.info(f"🔌 Спроба {retry_count + 1}/{max_retries}: підключення до {KAFKA_SERVER}...")
            
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                
                # Налаштування надійності
                acks='all',  # Чекаємо підтвердження від всіх реплік
                retries=5,   # Повтори у разі помилок
                
                # Налаштування порядку повідомлень
                max_in_flight_requests_per_connection=1,
                
                # Налаштування timeout
                request_timeout_ms=30000,
                
                # Compression для економії bandwidth
                compression_type='gzip',
            )
            
            logger.info(f"✅ З'єднано з Kafka: {KAFKA_SERVER}")
            return producer
            
        except NoBrokersAvailable as e:
            retry_count += 1
            logger.warning(f"⚠️  Kafka недоступна: {e}")
            
            if retry_count < max_retries:
                logger.info(f"⏳ Очікування {retry_delay} секунд перед наступною спробою...")
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ Вичерпано всі спроби підключення ({max_retries})")
                raise
        
        except Exception as e:
            retry_count += 1
            logger.error(f"❌ Помилка підключення до Kafka: {e}")
            
            if retry_count >= max_retries:
                raise
            
            time.sleep(retry_delay)
    
    raise Exception(f"❌ Не вдалося підключитися до Kafka після {max_retries} спроб")

# ==================== DATA STREAMING ====================

def stream_data():
    """
    Головна функція стрімінгу даних з CSV в Kafka.
    
    Процес:
        1. Читання CSV файлу
        2. Створення Kafka Producer
        3. Відправка даних з контрольованою швидкістю
        4. Логування прогресу
    """
    # 1. Перевірка наявності CSV файлу
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"❌ CSV файл не знайдено: {CSV_FILE}")
    
    # 2. Читання CSV
    logger.info(f"📖 Читання даних з: {CSV_FILE}")
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8')
        total_records = len(df)
        logger.info(f"📊 Знайдено {total_records} записів")
    except Exception as e:
        logger.error(f"❌ Помилка читання CSV: {e}")
        raise
    
    # 3. Створення Producer
    producer = create_producer()
    
    # 4. Розрахунок затримки між повідомленнями
    delay = 1.0 / MESSAGES_PER_SECOND
    
    # 5. Відправка даних
    sent_count = 0
    error_count = 0
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info(f"🚀 ПОЧАТОК ВІДПРАВКИ ДАНИХ")
    logger.info(f"   Топік: {KAFKA_TOPIC}")
    logger.info(f"   Швидкість: {MESSAGES_PER_SECOND} повідомлень/сек")
    logger.info(f"   Загальна кількість: {total_records}")
    logger.info("=" * 70)
    
    try:
        for index, row in df.iterrows():
            # Створення повідомлення (RAW дані, БЕЗ збагачення)
            message = {
                "author_id": str(row['author_id']),
                "created_at": str(row['created_at']),
                "text": str(row['text'])
            }
            
            try:
                # Відправка в Kafka (асинхронно)
                future = producer.send(KAFKA_TOPIC, value=message)
                
                # Чекаємо підтвердження (для гарантії доставки)
                record_metadata = future.get(timeout=10)
                
                sent_count += 1
                
                # Логування прогресу кожні 50 повідомлень
                if sent_count % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = sent_count / elapsed
                    
                    logger.info(
                        f"📤 Відправлено: {sent_count}/{total_records} "
                        f"({(sent_count/total_records*100):.1f}%) | "
                        f"Швидкість: {rate:.2f} msg/sec | "
                        f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}"
                    )
                
            except KafkaError as e:
                error_count += 1
                logger.error(f"❌ Помилка відправки повідомлення {index}: {e}")
                continue
            
            # Затримка для симуляції реального потоку
            time.sleep(delay)
        
        # Flush буфера (чекаємо відправки всіх повідомлень)
        logger.info("⏳ Flush буфера Producer...")
        producer.flush()
        
        # Фінальна статистика
        elapsed = time.time() - start_time
        actual_rate = sent_count / elapsed
        
        logger.info("=" * 70)
        logger.info("✅ ВІДПРАВКУ ЗАВЕРШЕНО")
        logger.info("=" * 70)
        logger.info(f"✅ Успішно відправлено: {sent_count}/{total_records}")
        logger.info(f"❌ Помилок: {error_count}")
        logger.info(f"⏱️  Час роботи: {elapsed:.2f} секунд")
        logger.info(f"⚡ Фактична швидкість: {actual_rate:.2f} msg/sec")
        logger.info("=" * 70)
        
    except KeyboardInterrupt:
        logger.info("⚠️  Зупинка Producer (отримано SIGINT)")
        logger.info(f"📊 Відправлено: {sent_count}/{total_records} перед зупинкою")
        
    finally:
        # Закриття Producer
        producer.close()
        logger.info("🏁 Producer зупинено")

# ==================== ENTRY POINT ====================

if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("🎬 KAFKA PRODUCER LAUNCHER - STARTING")
    logger.info("=" * 70)
    logger.info(f"Kafka Server: {KAFKA_SERVER}")
    logger.info(f"Target Topic: {KAFKA_TOPIC}")
    logger.info(f"Data Source: {CSV_FILE}")
    logger.info(f"Speed: {MESSAGES_PER_SECOND} messages/second")
    logger.info("=" * 70)
    
    try:
        stream_data()
    except Exception as e:
        logger.error(f"💥 Критична помилка: {e}", exc_info=True)
        exit(1)