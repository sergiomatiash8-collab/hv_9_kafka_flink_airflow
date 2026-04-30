import unittest
from datetime import datetime
from dataclasses import FrozenInstanceError
from src.domain.entities.tweet import Tweet
from src.domain.value_objects.author_id import AuthorId
from src.domain.value_objects.company import Company

class TestTweetImmutability(unittest.TestCase):

    def test_tweet_is_frozen(self):
        """Verify that it is impossible to modify an attribute of a created tweet."""
        tweet = Tweet(
            author_id=AuthorId("123"),
            created_at=datetime.now(),
            text="Initial text"
        )
        
        # Attempting to change the text should raise an Exception (specifically FrozenInstanceError)
        with self.assertRaises(Exception):
            tweet.text = "Changed text"

    def test_enrich_creates_new_instance(self):
        """Verify that the enrich method returns a NEW instance instead of modifying the old one."""
        original_tweet = Tweet(
            author_id=AuthorId("123"),
            created_at=datetime.now(),
            text="Hello"
        )
        
        new_tweet = original_tweet.enrich(
            company=Company.AMAZON,
            priority=None 
        )
        
        # Check that these are two different objects in memory
        self.assertIsNot(original_tweet, new_tweet)
        # Check that the original company remains empty (None)
        self.assertIsNone(original_tweet.company)
        # Check that the new instance has the company set
        self.assertEqual(new_tweet.company, Company.AMAZON)

if __name__ == "__main__":
    unittest.main()