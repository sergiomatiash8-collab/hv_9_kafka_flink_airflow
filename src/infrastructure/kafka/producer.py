import json
import logging
from kafka import KafkaProducer as KafkaClient
from typing import List

from ...domain.entities.tweet import Tweet
from ...domain.repositories.message_producer import TweetProducer

logger = logging.getLogger(__name__)

class KafkaProducerAdapter(TweetProducer):
    """
    Kafka adapter implementation.

    Implements TweetProducer interface while hiding Kafka details.
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
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                **kafka_config
            )
            logger.info(f"Kafka Producer initialized for {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def send(self, tweet: Tweet) -> None:
        """Send a single tweet."""
        message = self._serialize_tweet(tweet)
        self._producer.send(self._topic, value=message)

    def send_batch(self, tweets: List[Tweet]) -> None:
        """Send batch of messages with forced flush."""
        for tweet in tweets:
            self.send(tweet)
        self._producer.flush()

    def close(self) -> None:
        """Close connection."""
        self._producer.close()
        logger.info("Kafka connection closed")

    def _serialize_tweet(self, tweet: Tweet) -> dict:
        """
        Mapper: converts Domain Entity into Kafka-friendly dict format.

        Isolates Tweet structure from message format.
        """
        return {
            "author_id": tweet.author_id.value,
            "created_at": tweet.created_at.isoformat(),
            "text": tweet.text,
            "company": tweet.company.value if tweet.company else None,
            "priority": tweet.priority.value if tweet.priority else None,
            "text_length": tweet.text_length
        }