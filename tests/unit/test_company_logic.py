import unittest
from src.domain.value_objects.company import Company

class TestCompanyLogic(unittest.TestCase):

    def test_mapping_known_authors(self):
        """Verify that Apple and Uber IDs are recognized correctly."""
        # Apple ID from your code mapping
        self.assertEqual(Company.from_author_id("115854"), Company.APPLE)
        # Uber ID from your code mapping
        self.assertEqual(Company.from_author_id("17919972"), Company.UBER)

    def test_mapping_unknown_author_returns_other(self):
        """Verify that OTHER is returned for an unknown ID."""
        self.assertEqual(Company.from_author_id("999999"), Company.OTHER)

if __name__ == "__main__":
    unittest.main()