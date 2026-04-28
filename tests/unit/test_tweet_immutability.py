import unittest
from datetime import datetime
from src.domain.entities.tweet import Tweet
from src.domain.value_objects.author_id import AuthorId
from src.domain.value_objects.company import Company

class TestTweetImmutability(unittest.TestCase):

    def test_tweet_is_frozen(self):
        """Перевіряємо, що неможливо змінити атрибут створеного твіта."""
        tweet = Tweet(
            author_id=AuthorId("123"),
            created_at=datetime.now(),
            text="Initial text"
        )
        
        # Спроба змінити текст має викликати помилку FrozenInstanceError
        with self.assertRaises(Exception):
            tweet.text = "Changed text"

    def test_enrich_creates_new_instance(self):
        """Перевіряємо, що метод enrich повертає НОВИЙ об'єкт, а не змінює старий."""
        original_tweet = Tweet(
            author_id=AuthorId("123"),
            created_at=datetime.now(),
            text="Hello"
        )
        
        new_tweet = original_tweet.enrich(
            company=Company.AMAZON,
            priority=None # Припустимо, пріоритет поки неважливий
        )
        
        # Перевіряємо, що це два різні об'єкти в пам'яті
        self.assertIsNot(original_tweet, new_tweet)
        # Перевіряємо, що в оригінальному компанія залишилась порожньою
        self.assertIsNone(original_tweet.company)
        # А в новому вона встановлена
        self.assertEqual(new_tweet.company, Company.AMAZON)

if __name__ == "__main__":
    unittest.main()