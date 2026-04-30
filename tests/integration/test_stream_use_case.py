import unittest
from unittest.mock import MagicMock
from src.infrastructure.file_system.csv_reader import CSVTweetReader
from src.application.use_cases.stream_tweets import StreamTweetsUseCase
from src.infrastructure.config.settings import settings

class TestStreamUseCaseIntegration(unittest.TestCase):
    def test_flow_from_csv_to_use_case(self):
        # 1. Prepare the real reader
        reader = CSVTweetReader(settings.csv_file_path)
        
        # 2. Create a "mock" producer to avoid failure without Kafka
        mock_producer = MagicMock()
        
        # 3. Initialize our Use Case
        use_case = StreamTweetsUseCase(
            tweet_reader=reader,
            tweet_producer=mock_producer
        )
        
        # 4. Execute the scenario
        result = use_case.execute(enrich=False)
        
        # 5. Assertions
        self.assertEqual(result["status"], "success")
        self.assertGreater(result["processed_count"], 0)
        
        # Verify that the Use Case called the send_batch method on our mock producer
        mock_producer.send_batch.assert_called_once()

if __name__ == "__main__":
    unittest.main()