import logging
from typing import Protocol, List, Dict, Any, Optional

from ...domain.entities.tweet import Tweet
from ...domain.repositories.message_producer import TweetProducer
from ...domain.services.enrichment_service import EnrichmentService

logger = logging.getLogger(__name__)

class TweetReader(Protocol):
    """
    Вхідний порт (Input Port).
    Нам байдуже, звідки прийдуть твіти (CSV, API чи DB).
    """
    def read_tweets(self) -> List[Tweet]:
        ...

class StreamTweetsUseCase:
    """
    Use Case: Потік твітів від джерела до брокера повідомлень.
    Реалізує логіку: Зчитати -> (Опціонально) Збагатити -> Відправити.
    """

    def __init__(
        self,
        tweet_reader: TweetReader,
        tweet_producer: TweetProducer,
        enrichment_service: Optional[EnrichmentService] = None
    ):
        self._reader = tweet_reader
        self._producer = tweet_producer
        self._enrichment_service = enrichment_service

    def execute(self, enrich: bool = False) -> Dict[str, Any]:
        """
        Головний метод виконання сценарію.
        """
        logger.info("Запуск стрімінгу твітів...")
        
        try:
            # 1. Зчитування (через абстрактний рідер)
            tweets = self._reader.read_tweets()
            count = len(tweets)
            logger.info(f"Зчитано {count} твітів")

            # 2. Збагачення (якщо потрібно)
            if enrich and self._enrichment_service:
                tweets = self._enrichment_service.batch_enrich(tweets)
                logger.info("Твіти збагачено бізнес-логікою")

            # 3. Відправка в Kafka (через інтерфейс продюсера)
            self._producer.send_batch(tweets)
            logger.info(f"Успішно відправлено {count} повідомлень")

            return {
                "status": "success",
                "processed_count": count,
                "enriched": enrich
            }

        except Exception as e:
            logger.error(f"Помилка під час стрімінгу: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e)
            }
        finally:
            self._producer.close()