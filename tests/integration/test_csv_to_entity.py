import unittest
from src.infrastructure.file_system.csv_reader import CSVTweetReader
from src.domain.entities.tweet import Tweet
from src.infrastructure.config.settings import settings

class TestCsvToEntityIntegration(unittest.TestCase):
    def test_reader_converts_real_data(self):
        """
        Verify the real chain: 
        Reading file via CSVTweetReader -> Creating a Tweet object.
        """
        # Using the real reader instead of an abstract producer
        reader = CSVTweetReader(settings.csv_file_path)
        
        # Get the list of tweets
        tweets = reader.read_tweets()
        
        if not tweets:
            self.fail(f"CSV file at path {settings.csv_file_path} is empty or not found!")
            
        first_tweet = tweets[0]
        
        # Check that the output is specifically an instance of our Tweet class
        self.assertIsInstance(first_tweet, Tweet)
        
        # Verify that the internal data is valid
        self.assertIsNotNone(first_tweet.author_id.value)
        self.assertIsNotNone(first_tweet.text)
        self.assertIsNotNone(first_tweet.created_at)
        
        # Additional check: verify if the company was identified
        # (Kaggle datasets usually start with companies like Apple or Uber)
        self.assertIsNotNone(first_tweet.company)

if __name__ == "__main__":
    unittest.main()