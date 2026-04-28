import unittest
from unittest.mock import MagicMock
from src.infrastructure.file_system.csv_reader import CSVTweetReader
from src.application.use_cases.stream_tweets import StreamTweetsUseCase
from src.infrastructure.config.settings import settings

class TestStreamUseCaseIntegration(unittest.TestCase):
    def test_flow_from_csv_to_use_case(self):
        # 1. Готуємо реальний рідер
        reader = CSVTweetReader(settings.csv_file_path)
        
        # 2. Створюємо "фейковий" продюсер (заглушку), щоб не впасти без Kafka
        mock_producer = MagicMock()
        
        # 3. Ініціалізуємо наш Use Case
        use_case = StreamTweetsUseCase(
            tweet_reader=reader,
            tweet_producer=mock_producer
        )
        
        # 4. Виконуємо сценарій
        result = use_case.execute(enrich=False)
        
        # 5. Перевірки
        self.assertEqual(result["status"], "success")
        self.assertGreater(result["processed_count"], 0)
        
        # Перевіряємо, чи викликав Use Case метод send_batch у нашого "фейкового" продюсера
        mock_producer.send_batch.assert_called_once()

if __name__ == "__main__":
    unittest.main()