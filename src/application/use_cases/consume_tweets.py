"""
Use Case для споживання збагачених твітів з Kafka.

Відповідальності:
    1. Читання збагачених даних з топіку 'tweets_enriched'
    2. Валідація та перетворення у Domain Entity
    3. Збереження в PostgreSQL
    4. Збереження в CSV файли (по хвилинах)

Clean Architecture шар: Application Layer
"""

import logging
from datetime import datetime
from typing import Optional
from src.domain.entities.tweet import Tweet
from src.infrastructure.kafka.consumer import KafkaInfrastructureConsumer
from src.infrastructure.repositories.postgres_repository import PostgresTweetRepository
from src.infrastructure.repositories.csv_repository import CSVTweetRepository
from src.domain.value_objects.author_id import AuthorId
from src.domain.value_objects.company import Company
from src.domain.value_objects.priority import Priority

logger = logging.getLogger(__name__)

class ConsumeEnrichedTweetsUseCase:
    """
    Use Case для споживання ЗБАГАЧЕНИХ твітів.
    
    Архітектура потоку:
        Kafka (tweets_enriched) → Consumer → [PostgreSQL + CSV]
    """
    
    def __init__(self):
        """
        Ініціалізація залежностей.
        
        Dependency Injection через конструктор (можна додати DI контейнер).
        """
        # ✅ ВИПРАВЛЕНО: правильний топік
        self.kafka_reader = KafkaInfrastructureConsumer(topic="tweets_enriched")
        self.db_repo = PostgresTweetRepository()
        self.csv_repo = CSVTweetRepository()
        
        # Метрики для моніторингу
        self.processed_count = 0
        self.error_count = 0
        self.start_time = datetime.now()
        
        logger.info("=" * 70)
        logger.info("🚀 ENRICHED TWEETS CONSUMER - INITIALIZED")
        logger.info("=" * 70)
        logger.info(f"📥 Reading from topic: tweets_enriched")
        logger.info(f"💾 Saving to: PostgreSQL + CSV files")
        logger.info("=" * 70)

    def execute(self):
        """
        Головний цикл обробки збагачених твітів.
        
        Працює безкінечно (blocking) до отримання SIGINT.
        """
        logger.info("🔄 Початок споживання повідомлень...")
        
        try:
            for raw_data in self.kafka_reader.consume():
                self._process_message(raw_data)
                
        except KeyboardInterrupt:
            logger.info("⚠️  Отримано сигнал зупинки (CTRL+C)")
            self._log_statistics()
        except Exception as e:
            logger.error(f"💥 Критична помилка в Consumer: {e}", exc_info=True)
            raise
        finally:
            self._cleanup()
    
    def _process_message(self, raw_data: dict) -> None:
        """
        Обробка одного повідомлення з Kafka.
        
        Args:
            raw_data: Словник з даними з Kafka
        """
        try:
            # 1. Валідація обов'язкових полів
            if not self._validate_message(raw_data):
                return
            
            # 2. Перетворення у Domain Entity
            tweet = self._map_to_entity(raw_data)
            if not tweet:
                return
            
            # 3. Збереження в PostgreSQL
            self.db_repo.save(tweet)
            
            # 4. Збереження в CSV
            self.csv_repo.save(tweet)
            
            # 5. Оновлення метрик
            self.processed_count += 1
            
            # Логування кожні 100 повідомлень
            if self.processed_count % 100 == 0:
                self._log_progress()
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"❌ Помилка обробки повідомлення: {e}")
            logger.debug(f"Дані: {raw_data}")
    
    def _validate_message(self, data: dict) -> bool:
        """
        Валідація структури повідомлення.
        
        Args:
            data: Дані з Kafka
            
        Returns:
            True якщо повідомлення валідне, False інакше
        """
        required_fields = ['author_id', 'created_at', 'text']
        
        for field in required_fields:
            if field not in data or not data[field]:
                logger.warning(f"⚠️  Пропущено повідомлення: відсутнє поле '{field}'")
                return False
        
        return True
    
    def _map_to_entity(self, data: dict) -> Optional[Tweet]:
        """
        Маппінг RAW даних у Domain Entity.
        
        Args:
            data: Словник з Kafka
            
        Returns:
            Tweet entity або None у разі помилки
        """
        try:
            # Парсинг дати (підтримка різних форматів)
            created_at = self._parse_datetime(data['created_at'])
            
            # Створення Tweet Entity
            tweet = Tweet(
                author_id=AuthorId(data['author_id']),
                created_at=created_at,
                text=data['text'],
                
                # ✅ Збагачені поля з Flink
                company=Company(data['company']) if data.get('company') else None,
                priority=Priority(data['priority']) if data.get('priority') else None
            )
            
            return tweet
            
        except Exception as e:
            logger.error(f"❌ Помилка маппінгу даних в Entity: {e}")
            return None
    
    def _parse_datetime(self, date_str: str) -> datetime:
        """
        Парсинг дати з підтримкою різних форматів.
        
        Args:
            date_str: Рядок з датою
            
        Returns:
            datetime об'єкт
        """
        formats = [
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Fallback на ISO format
        return datetime.fromisoformat(date_str)
    
    def _log_progress(self) -> None:
        """Логування прогресу обробки."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.processed_count / elapsed if elapsed > 0 else 0
        
        logger.info(
            f"📊 Прогрес: {self.processed_count} оброблено "
            f"({rate:.2f} msg/sec), помилок: {self.error_count}"
        )
    
    def _log_statistics(self) -> None:
        """Логування фінальної статистики."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        logger.info("=" * 70)
        logger.info("📊 ФІНАЛЬНА СТАТИСТИКА")
        logger.info("=" * 70)
        logger.info(f"✅ Успішно оброблено: {self.processed_count}")
        logger.info(f"❌ Помилок: {self.error_count}")
        logger.info(f"⏱️  Час роботи: {elapsed:.2f} секунд")
        logger.info(f"⚡ Середня швидкість: {self.processed_count/elapsed:.2f} msg/sec")
        logger.info("=" * 70)
    
    def _cleanup(self) -> None:
        """Очищення ресурсів."""
        logger.info("🧹 Очищення ресурсів...")
        # Тут можна додати закриття з'єднань, flush буферів і т.д.