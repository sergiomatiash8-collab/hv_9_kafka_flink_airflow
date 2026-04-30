import json
import logging
from kafka import KafkaConsumer
from src.infrastructure.config.settings import settings

logger = logging.getLogger(__name__)

class KafkaInfrastructureConsumer:
    """
    Infrastructure implementation for consuming data from Kafka.

    This class does not know anything about business logic.
    It only knows how to read messages from Kafka.
    """

    def __init__(self, topic: str = None):
        # If topic is not provided, use default from settings
        self.topic = topic or settings.kafka.topic

        self._consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=settings.kafka.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id=settings.kafka.group_id,
            enable_auto_commit=True
        )

    def consume(self):
        """Generator that yields messages one by one."""
        logger.info(f"Starting consumption from topic: {self.topic}")
        try:
            for message in self._consumer:
                # Return only payload
                yield message.value
        except Exception as e:
            logger.error(f"Error while consuming from Kafka: {e}")
            raise