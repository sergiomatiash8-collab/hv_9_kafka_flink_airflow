import unittest
from datetime import datetime
from src.domain.entities.tweet import Tweet
from src.domain.value_objects.author_id import AuthorId
from src.domain.value_objects.priority import Priority

class TestPriorityLogic(unittest.TestCase):

    def test_default_priority_is_none(self):
        """Verify that priority is not set by default."""
        tweet = Tweet(
            author_id=AuthorId("123"),
            created_at=datetime.now(),
            text="Test priority"
        )
        self.assertIsNone(tweet.priority)

    def test_setting_valid_priority(self):
        """Verify setting a specific priority."""
        # Ensure that Priority.HIGH or similar exists in your priority.py
        # If names differ, replace them here
        target_priority = Priority.HIGH if hasattr(Priority, 'HIGH') else list(Priority)[0]
        
        tweet = Tweet(
            author_id=AuthorId("123"),
            created_at=datetime.now(),
            text="Test priority",
            priority=target_priority
        )
        self.assertEqual(tweet.priority, target_priority)

if __name__ == "__main__":
    unittest.main()