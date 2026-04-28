import unittest
from datetime import datetime
from src.domain.entities.tweet import Tweet
from src.domain.value_objects.author_id import AuthorId
from src.domain.value_objects.company import Company
from src.domain.value_objects.priority import Priority

class TestTweetEntity(unittest.TestCase):

    def test_tweet_creation_success(self):
        """Перевіряємо, що валідний твіт створюється без помилок."""
        author = AuthorId("123")
        text = "Valid tweet text"
        tweet = Tweet(
            author_id=author,
            created_at=datetime.now(),
            text=text,
            company=Company.APPLE,
            priority=Priority.HIGH
        )
        self.assertEqual(tweet.text, text)
        self.assertEqual(tweet.author_id.value, "123")

    def test_tweet_empty_text_raises_error(self):
        """Перевіряємо, чи спрацює валідація на порожній текст."""
        with self.assertRaises(ValueError) as context:
            Tweet(
                author_id=AuthorId("123"),
                created_at=datetime.now(),
                text="",  # ПОМИЛКА ТУТ
                company=Company.OTHER
            )
        self.assertIn("Текст твіта не може бути порожнім", str(context.exception))

    def test_tweet_too_long_raises_error(self):
        """Перевіряємо ліміт у 280 символів."""
        long_text = "a" * 281
        with self.assertRaises(ValueError) as context:
            Tweet(
                author_id=AuthorId("123"),
                created_at=datetime.now(),
                text=long_text
            )
        self.assertIn("перевищує ліміт 280 символів", str(context.exception))

if __name__ == "__main__":
    unittest.main()