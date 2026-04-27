import pytest
from unittest.mock import Mock
from datetime import datetime

from src.application.use_cases.stream_tweets import StreamTweetsUseCase
from src.domain.entities.tweet import Tweet
from src.domain.value_objects.author_id import AuthorId

class TestStreamTweetsUseCase:
    """
    Тестування Use Case за допомогою Моків (Mocks).
    Ми перевіряємо не дані, а ПОВЕДІНКУ системи.
    """

    def test_execute_success(self):
        """Тест успішного сценарію: зчитав -> збагатив -> відправив."""
        # Arrange: Створюємо "фейкові" об'єкти (заглушки)
        mock_reader = Mock()
        mock_producer = Mock()
        mock_enrichment = Mock()

        # Налаштовуємо поведінку: що мають повертати методи
        test_tweets = [
            Tweet(AuthorId("1"), datetime.now(), "Test 1"),
            Tweet(AuthorId("2"), datetime.now(), "Test 2"),
        ]
        mock_reader.read_tweets.return_value = test_tweets
        mock_enrichment.batch_enrich.return_value = test_tweets

        # Ініціалізуємо Use Case з моками
        use_case = StreamTweetsUseCase(
            tweet_reader=mock_reader,
            tweet_producer=mock_producer,
            enrichment_service=mock_enrichment
        )

        # Act: Виконуємо сценарій
        result = use_case.execute(enrich=True)

        # Assert: Перевіряємо статус та те, чи викликалися потрібні методи
        assert result['status'] == 'success'
        assert result['processed_count'] == 2
        
        # Перевіряємо контракт: чи всі етапи пройшли
        mock_reader.read_tweets.assert_called_once()
        mock_enrichment.batch_enrich.assert_called_once_with(test_tweets)
        mock_producer.send_batch.assert_called_once_with(test_tweets)
        mock_producer.close.assert_called_once()

    def test_execute_error_handling(self):
        """Тест обробки помилок: якщо рідер впаде, продюсер має закритися."""
        # Arrange
        mock_reader = Mock()
        mock_producer = Mock()
        mock_reader.read_tweets.side_effect = Exception("File read error")

        use_case = StreamTweetsUseCase(mock_reader, mock_producer)

        # Act
        result = use_case.execute()

        # Assert
        assert result['status'] == 'error'
        assert "File read error" in result['message']
        # КРИТИЧНО: Продюсер має бути закритий навіть при помилці
        mock_producer.close.assert_called_once()