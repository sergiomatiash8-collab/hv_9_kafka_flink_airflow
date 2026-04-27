import logging
import sys

from ...application.use_cases.stream_tweets import StreamTweetsUseCase
from ...domain.services.enrichment_service import EnrichmentService
from ...infrastructure.kafka.producer import KafkaProducerAdapter
from ...infrastructure.file_system.csv_reader import CSVTweetReader
from ...infrastructure.config.settings import settings

# Налаштування логування згідно з конфігурацією
logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    Main entry point.
    Тут ми з'єднуємо інтерфейси з реальною інфраструктурою.
    """
    logger.info("Ініціалізація додатка...")

    try:
        # 1. Ініціалізація інфраструктурних адаптерів (Infrastructure)
        tweet_reader = CSVTweetReader(settings.csv_file_path)
        
        tweet_producer = KafkaProducerAdapter(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            topic=settings.kafka.topic,
        )

        # 2. Ініціалізація доменних сервісів (Domain)
        enrichment_service = EnrichmentService()

        # 3. Збірка Use Case (Dependency Injection)
        # Ми передаємо конкретні класи туди, де очікуються інтерфейси
        use_case = StreamTweetsUseCase(
            tweet_reader=tweet_reader,
            tweet_producer=tweet_producer,
            enrichment_service=enrichment_service
        )

        # 4. Виконання сценарію
        logger.info("Запуск бізнес-сценарію стрімінгу...")
        result = use_case.execute(enrich=True)

        # 5. Обробка результату
        if result['status'] == 'success':
            logger.info(f"✅ Успішно оброблено {result['processed_count']} твітів")
            sys.exit(0)
        else:
            logger.error(f"❌ Помилка виконання: {result.get('message')}")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Процес перервано користувачем")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Критична помилка під час запуску: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()