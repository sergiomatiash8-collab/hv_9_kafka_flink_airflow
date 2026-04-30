import logging
from typing import Protocol, List, Dict, Any, Optional

from ...domain.entities.tweet import Tweet
from ...domain.repositories.message_producer import TweetProducer
from ...domain.services.enrichment_service import EnrichmentService

logger = logging.getLogger(__name__)

class TweetReader(Protocol):
    """
    Input port.
    It does not matter where tweets come from (CSV, API or DB).
    """
    def read_tweets(self) -> List[Tweet]:
        ...

class StreamTweetsUseCase:
    """
    Use Case: Stream tweets from a source to a message broker.
    Implements logic: Read -> (Optional) Enrich -> Send.
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
        Main execution method.
        """
        logger.info("Starting tweet streaming...")

        try:
            # 1. Read (via abstract reader)
            tweets = self._reader.read_tweets()
            count = len(tweets)
            logger.info(f"Read {count} tweets")

            # 2. Enrichment (optional)
            if enrich and self._enrichment_service:
                tweets = self._enrichment_service.batch_enrich(tweets)
                logger.info("Tweets enriched with business logic")

            # 3. Send to Kafka (via producer interface)
            self._producer.send_batch(tweets)
            logger.info(f"Successfully sent {count} messages")

            return {
                "status": "success",
                "processed_count": count,
                "enriched": enrich
            }

        except Exception as e:
            logger.error(f"Streaming error: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e)
            }
        finally:
            self._producer.close()