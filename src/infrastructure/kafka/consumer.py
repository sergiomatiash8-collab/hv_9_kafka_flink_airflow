import json
import logging
from kafka import KafkaConsumer
# Імпортуємо наш синглтон налаштувань
from src.infrastructure.config.settings import settings

logger = logging.getLogger(__name__)

class KafkaInfrastructureConsumer:
    """
    Інфраструктурна реалізація для отримання даних з Kafka.
    Клас не знає про логіку обробки, він лише вміє тягнути повідомлення.
    """
    
    def __init__(self, topic: str = None):
        # Якщо топік не передано, беремо дефолтний з налаштувань
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
        """Генератор, який віддає повідомлення по одному"""
        logger.info(f"[*] Starting consumption from topic: {self.topic}")
        try:
            for message in self._consumer:
                # Повертаємо тільки корисне навантаження (payload)
                yield message.value
        except Exception as e:
            logger.error(f"[-] Error while consuming from Kafka: {e}")
            raise