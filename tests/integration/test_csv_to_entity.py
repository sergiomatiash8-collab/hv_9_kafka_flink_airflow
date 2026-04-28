import unittest
from src.infrastructure.file_system.csv_reader import CSVTweetReader
from src.domain.entities.tweet import Tweet
from src.infrastructure.config.settings import settings

class TestCsvToEntityIntegration(unittest.TestCase):
    def test_reader_converts_real_data(self):
        """
        Перевіряємо реальний ланцюг: 
        Зчитування файлу через CSVTweetReader -> Створення об'єкта Tweet.
        """
        # Використовуємо реальний рідер замість абстрактного продюсера
        reader = CSVTweetReader(settings.csv_file_path)
        
        # Отримуємо список твітів
        tweets = reader.read_tweets()
        
        if not tweets:
            self.fail(f"CSV файл за шляхом {settings.csv_file_path} порожній або не знайдений!")
            
        first_tweet = tweets[0]
        
        # Перевіряємо, що на виході саме об'єкт нашого класу Tweet
        self.assertIsInstance(first_tweet, Tweet)
        
        # Перевіряємо, що дані всередині валідні
        self.assertIsNotNone(first_tweet.author_id.value)
        self.assertIsNotNone(first_tweet.text)
        self.assertIsNotNone(first_tweet.created_at)
        
        # Додаткова крута перевірка: чи визначилась компанія
        # (в Kaggle на початку файлу зазвичай Apple/Uber)
        self.assertIsNotNone(first_tweet.company)

if __name__ == "__main__":
    unittest.main()