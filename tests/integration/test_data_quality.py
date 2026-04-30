import unittest
import pandas as pd
from src.infrastructure.config.settings import settings

class TestDataQuality(unittest.TestCase):
    """
    Data Quality Tests (DQT).
    Verifying the integrity and validity of input data before processing.
    """

    @classmethod
    def setUpClass(cls):
        """Load dataset once for all tests."""
        try:
            cls.df = pd.read_csv(settings.csv_file_path)
        except Exception as e:
            raise unittest.SkipTest(f"Unable to load CSV: {e}")

    def test_completeness_no_nulls(self):
        """Completeness check: critical fields must not contain NaN."""
        critical_columns = ['author_id', 'created_at', 'text']
        for col in critical_columns:
            null_count = self.df[col].isnull().sum()
            with self.subTest(column=col):
                self.assertEqual(null_count, 0, f"Column {col} contains {null_count} null values!")

    def test_validity_author_id(self):
        """Validity check: author_id must not be an empty string."""
        # Convert to string, strip whitespace, and check for empty values
        invalid_ids = self.df[self.df['author_id'].astype(str).str.strip() == ''].shape[0]
        self.assertEqual(invalid_ids, 0, f"Found {invalid_ids} rows with empty author_id.")

    def test_text_uniqueness_ratio(self):
        """Uniqueness check: verify if there are too many duplicate texts (spam indicator)."""
        total_count = len(self.df)
        unique_count = self.df['text'].nunique()
        uniqueness_ratio = unique_count / total_count
        
        # If unique tweets are less than 50%, it's suspicious for this dataset
        self.assertGreater(uniqueness_ratio, 0.5, f"Data uniqueness ratio is too low: {uniqueness_ratio:.2f}")

    def test_text_content_validity(self):
        """Content validity check: text must have a reasonable length."""
        texts = self.df['text'].astype(str)
        
        # Check for excessively short tweets (e.g., 1 character)
        too_short = texts[texts.str.len() < 2].count()
        self.assertEqual(too_short, 0, f"Found {too_short} excessively short messages.")

if __name__ == "__main__":
    unittest.main()