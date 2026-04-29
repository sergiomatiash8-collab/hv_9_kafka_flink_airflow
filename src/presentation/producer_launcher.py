import time
import sys
import os
import logging

# Додаємо корінь проєкту в PATH, щоб імпорти точно працювали на Windows
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Налаштування логів, щоб бачити прогрес у консолі
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Виправлені імпорти згідно з твоїм ls
from src.application.use_cases.stream_tweets import StreamTweetsUseCase
from src.infrastructure.kafka.producer import KafkaProducerAdapter
from src.infrastructure.file_system.csv_reader import CSVTweetReader
from src.infrastructure.config.settings import settings

if __name__ == "__main__":
    logger.info("🚀 Запуск лаунчера продусера...")
    
    # Чекаємо, поки Kafka підніметься в Docker
    logger.info("Waiting 10 seconds for Kafka to be ready...")
    time.sleep(10)
    
    try:
        logger.info(f"Читання файлу: {settings.csv_file_path}")
        reader = CSVTweetReader(settings.csv_file_path)
        
        logger.info("Ініціалізація Kafka Producer...")
        # Переконайся, що в settings прописано bootstrap_servers='localhost:9092'
        producer = KafkaProducerAdapter(
            bootstrap_servers="localhost:9092", 
            topic="tweets"
        )
        
        logger.info("Запуск UseCase відправки даних...")
        use_case = StreamTweetsUseCase(reader, producer)
        use_case.execute()
        
        logger.info("✅ Обробка завершена успішно!")
        
    except Exception as e:
        logger.error(f"❌ Критична помилка: {e}", exc_info=True)
    finally:
        if 'producer' in locals():
            producer.close()