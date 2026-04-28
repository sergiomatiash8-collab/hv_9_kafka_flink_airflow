import logging
from src.domain.entities.tweet import Tweet
from src.infrastructure.kafka.consumer import KafkaInfrastructureConsumer
from src.infrastructure.repositories.postgres_repository import PostgresTweetRepository
from src.infrastructure.repositories.csv_repository import CSVTweetRepository
from src.domain.value_objects.author_id import AuthorId
from src.domain.value_objects.company import Company
from src.domain.value_objects.priority import Priority
from datetime import datetime

logger = logging.getLogger(__name__)

class ConsumeEnrichedTweetsUseCase:
    def __init__(self):
        # Ініціалізуємо інфраструктуру
        self.kafka_reader = KafkaInfrastructureConsumer(topic="enriched_tweets")
        self.db_repo = PostgresTweetRepository()
        self.csv_repo = CSVTweetRepository()

    def execute(self):
        """Головний цикл обробки готових даних"""
        logger.info("[*] Use Case: Початок вичитки оброблених твітів...")
        
        for raw_data in self.kafka_reader.consume():
            try:
                # 1. Перетворюємо сирі дані з Kafka у нашу Entity
                # Примітка: тут ми маємо точно знати формат, який видає Flink
                tweet = Tweet(
                    author_id=AuthorId(raw_data['author_id']),
                    created_at=datetime.fromisoformat(raw_data['created_at']),
                    text=raw_data['text'],
                    company=Company(raw_data['company']) if raw_data.get('company') else None,
                    priority=Priority(raw_data['priority']) if raw_data.get('priority') else None
                )

                # 2. Зберігаємо в БД
                self.db_repo.save(tweet)

                # 3. Зберігаємо в CSV
                self.csv_repo.save(tweet)

                logger.debug(f"[+] Оброблено твіт від {tweet.author_id}")

            except Exception as e:
                logger.error(f"[-] Помилка при обробці повідомлення: {e}")
                continue