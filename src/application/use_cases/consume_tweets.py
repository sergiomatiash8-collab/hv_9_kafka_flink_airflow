"""
Use Case for consuming enriched tweets from Kafka.

Responsibilities:
    1. Read enriched data from 'tweets_enriched' topic
    2. Validate and map to Domain Entity
    3. Save to PostgreSQL
    4. Save to CSV files (by minute)

Clean Architecture layer: Application Layer
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
    Use Case for consuming enriched tweets.

    Data flow architecture:
        Kafka (tweets_enriched) → Consumer → [PostgreSQL + CSV]
    """

    def __init__(self):
        """
        Dependency initialization.

        Constructor-based dependency injection (DI container can be added later).
        """
        # Correct topic
        self.kafka_reader = KafkaInfrastructureConsumer(topic="tweets_enriched")
        self.db_repo = PostgresTweetRepository()
        self.csv_repo = CSVTweetRepository()

        # Metrics for monitoring
        self.processed_count = 0
        self.error_count = 0
        self.start_time = datetime.now()

        logger.info("=" * 70)
        logger.info("ENRICHED TWEETS CONSUMER INITIALIZED")
        logger.info("=" * 70)
        logger.info("Reading from topic: tweets_enriched")
        logger.info("Saving to: PostgreSQL + CSV files")
        logger.info("=" * 70)

    def execute(self):
        """
        Main processing loop for enriched tweets.

        Runs indefinitely (blocking) until SIGINT.
        """
        logger.info("Starting message consumption loop...")

        try:
            for raw_data in self.kafka_reader.consume():
                self._process_message(raw_data)

        except KeyboardInterrupt:
            logger.info("Stop signal received (CTRL+C)")
            self._log_statistics()
        except Exception as e:
            logger.error(f"Critical Consumer error: {e}", exc_info=True)
            raise
        finally:
            self._cleanup()

    def _process_message(self, raw_data: dict) -> None:
        """
        Process a single Kafka message.

        Args:
            raw_data: Dictionary from Kafka
        """
        try:
            # 1. Validate required fields
            if not self._validate_message(raw_data):
                return

            # 2. Map to Domain Entity
            tweet = self._map_to_entity(raw_data)
            if not tweet:
                return

            # 3. Save to PostgreSQL
            self.db_repo.save(tweet)

            # 4. Save to CSV
            self.csv_repo.save(tweet)

            # 5. Update metrics
            self.processed_count += 1

            # Log every 100 messages
            if self.processed_count % 100 == 0:
                self._log_progress()

        except Exception as e:
            self.error_count += 1
            logger.error(f"Message processing error: {e}")
            logger.debug(f"Raw data: {raw_data}")

    def _validate_message(self, data: dict) -> bool:
        """
        Validate message structure.

        Args:
            data: Kafka message data

        Returns:
            True if valid, False otherwise
        """
        required_fields = ['author_id', 'created_at', 'text']

        for field in required_fields:
            if field not in data or not data[field]:
                logger.warning(f"Skipping message: missing field '{field}'")
                return False

        return True

    def _map_to_entity(self, data: dict) -> Optional[Tweet]:
        """
        Map raw data to Domain Entity.

        Args:
            data: Kafka dictionary

        Returns:
            Tweet entity or None on error
        """
        try:
            created_at = self._parse_datetime(data['created_at'])

            tweet = Tweet(
                author_id=AuthorId(data['author_id']),
                created_at=created_at,
                text=data['text'],

                # Enriched fields from Flink
                company=Company(data['company']) if data.get('company') else None,
                priority=Priority(data['priority']) if data.get('priority') else None
            )

            return tweet

        except Exception as e:
            logger.error(f"Entity mapping error: {e}")
            return None

    def _parse_datetime(self, date_str: str) -> datetime:
        """
        Parse datetime with multiple format support.
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

        return datetime.fromisoformat(date_str)

    def _log_progress(self) -> None:
        """Log processing progress."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.processed_count / elapsed if elapsed > 0 else 0

        logger.info(
            f"Progress: {self.processed_count} processed "
            f"({rate:.2f} msg/sec), errors: {self.error_count}"
        )

    def _log_statistics(self) -> None:
        """Log final statistics."""
        elapsed = (datetime.now() - self.start_time).total_seconds()

        logger.info("=" * 70)
        logger.info("FINAL STATISTICS")
        logger.info("=" * 70)
        logger.info(f"Processed: {self.processed_count}")
        logger.info(f"Errors: {self.error_count}")
        logger.info(f"Runtime: {elapsed:.2f} seconds")
        logger.info(f"Avg speed: {self.processed_count/elapsed:.2f} msg/sec")
        logger.info("=" * 70)

    def _cleanup(self) -> None:
        """Resource cleanup."""
        logger.info("Cleaning up resources...")
        # Close connections / flush buffers if needed