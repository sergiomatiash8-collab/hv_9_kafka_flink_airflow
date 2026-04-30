import unittest
from datetime import datetime
from src.domain.entities.tweet import Tweet
from src.domain.value_objects.author_id import AuthorId
from src.domain.value_objects.company import Company
from src.domain.value_objects.priority import Priority

class TestTweetEntity(unittest.TestCase):

    def test_tweet_creation_success(self):
        """Verify that a valid tweet is created without errors."""
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
        """Verify that validation triggers for empty text."""
        with self.assertRaises(ValueError) as context:
            Tweet(
                author_id=AuthorId("123"),
                created_at=datetime.now(),
                text="",  # ERROR HERE
                company=Company.OTHER
            )
        self.assertIn("Tweet text cannot be empty", str(context.exception))

    def test_tweet_too_long_raises_error(self):
        """Verify the 280 character limit."""
        long_text = "a" * 281
        with self.assertRaises(ValueError) as context:
            Tweet(
                author_id=AuthorId("123"),
                created_at=datetime.now(),
                text=long_text
            )
        self.assertIn("exceeds the 280 character limit", str(context.exception))

if __name__ == "__main__":
    unittest.main()