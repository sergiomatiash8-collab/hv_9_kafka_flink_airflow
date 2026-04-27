import pytest
from datetime import datetime

from src.domain.entities.tweet import Tweet
from src.domain.value_objects.author_id import AuthorId
from src.domain.value_objects.company import Company
from src.domain.value_objects.priority import Priority
from src.domain.services.enrichment_service import EnrichmentService

class TestEnrichmentService:
    """
    Unit-тести для перевірки ізольованої бізнес-логіки.
    Запускаються за мілісекунди.
    """

    def setup_method(self):
        """Ініціалізація сервісу перед кожним тестом."""
        self.service = EnrichmentService()

    def test_enrich_amazon_tweet(self):
        """Перевірка автоматичного визначення компанії Amazon та високого пріоритету."""
        # Arrange (Підготовка)
        tweet = Tweet(
            author_id=AuthorId("115852"),
            created_at=datetime.now(),
            text="My package is broken"
        )

        # Act (Дія)
        enriched = self.service.enrich_tweet(tweet)

        # Assert (Перевірка)
        assert enriched.company == Company.AMAZON
        assert enriched.priority == Priority.HIGH
        assert enriched.text_length == len(tweet.text)

    def test_high_priority_keywords(self):
        """Перевірка реакції на критичні ключові слова (crash, error)."""
        # Arrange
        tweet = Tweet(
            author_id=AuthorId("999"), # Невідомий автор
            created_at=datetime.now(),
            text="The app crashed with an error"
        )

        # Act
        enriched = self.service.enrich_tweet(tweet)

        # Assert
        assert enriched.company == Company.OTHER
        assert enriched.priority == Priority.HIGH
        assert enriched.is_high_priority is True

    def test_batch_enrichment(self):
        """Перевірка пакетної обробки списку твітів."""
        # Arrange
        tweets = [
            Tweet(AuthorId("115852"), datetime.now(), "Amazon issue"),
            Tweet(AuthorId("115854"), datetime.now(), "Apple help"),
        ]

        # Act
        enriched = self.service.batch_enrich(tweets)

        # Assert
        assert len(enriched) == 2
        assert enriched[0].company == Company.AMAZON
        assert enriched[1].company == Company.APPLE
        assert enriched[1].priority == Priority.MEDIUM