import json
import logging
from kafka import KafkaProducer as KafkaClient
from typing import List

from ...domain.entities.tweet import Tweet
from ...domain.repositories.message_producer import TweetProducer

logger = logging.getLogger(__name__)

class KafkaProducerAdapter(TweetProducer):
    """
    Адаптер для Kafka.
    Реалізує інтерфейс TweetProducer, приховуючи деталі реалізації Kafka.
    """

    def __init__(
        self, 
        bootstrap_servers: str, 
        topic: str, 
        **kafka_config
    ):
        self._topic = topic
        try:
            self._producer = KafkaClient(
                bootstrap_servers=bootstrap_servers,
                # Серіалізуємо словник у JSON-байтовий рядок
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                **kafka_config
            )
            logger.info(f"Kafka Producer ініціалізовано для {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Не вдалося підключитися до Kafka: {e}")
            raise

    def send(self, tweet: Tweet) -> None:
        """Відправка одного твіта."""
        message = self._serialize_tweet(tweet)
        self._producer.send(self._topic, value=message)

    def send_batch(self, tweets: List[Tweet]) -> None:
        """Відправка пачки повідомлень з примусовим очищенням буфера (flush)."""
        for tweet in tweets:
            self.send(tweet)
        self._producer.flush()  # Гарантуємо відправку всіх повідомлень з буфера

    def close(self) -> None:
        """Закриття з'єднання."""
        self._producer.close()
        logger.info("З'єднання з Kafka закрите")

    def _serialize_tweet(self, tweet: Tweet) -> dict:
        """
        Mapper: Перетворює Domain Entity у формат, який "розуміє" Kafka (dict).
        Ізолює структуру об'єкта Tweet від формату повідомлення в черзі.
        """
        return {
            "author_id": tweet.author_id.value,
            "created_at": tweet.created_at.isoformat(),
            "text": tweet.text,
            "company": tweet.company.value if tweet.company else None,
            "priority": tweet.priority.value if tweet.priority else None,
            "text_length": tweet.text_length
        }
