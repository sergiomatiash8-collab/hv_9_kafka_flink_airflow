import unittest
import pandas as pd
from src.infrastructure.config.settings import settings

class TestDataQuality(unittest.TestCase):
    """
    Data Quality Tests (DQT).
    Перевіряємо цілісність та валідність вхідних даних перед їх обробкою.
    """

    @classmethod
    def setUpClass(cls):
        """Завантажуємо датасет один раз для всіх тестів."""
        try:
            cls.df = pd.read_csv(settings.csv_file_path)
        except Exception as e:
            raise unittest.SkipTest(f"Неможливо завантажити CSV: {e}")

    def test_completeness_no_nulls(self):
        """Перевірка на повноту (Completeness): критичні поля не повинні мати NaN."""
        critical_columns = ['author_id', 'created_at', 'text']
        for col in critical_columns:
            null_count = self.df[col].isnull().sum()
            with self.subTest(column=col):
                self.assertEqual(null_count, 0, f"Колонка {col} містить {null_count} порожніх значень!")

    def test_validity_author_id(self):
        """Перевірка на валідність (Validity): author_id не повинен бути порожнім рядком."""
        # Перетворюємо в string, прибираємо пробіли і перевіряємо, чи не порожньо
        invalid_ids = self.df[self.df['author_id'].astype(str).str.strip() == ''].shape[0]
        self.assertEqual(invalid_ids, 0, f"Знайдено {invalid_ids} рядків з порожнім author_id.")

    def test_text_uniqueness_ratio(self):
        """Перевірка на унікальність (Uniqueness): чи не забагато дублікатів тексту (ознака спаму)."""
        total_count = len(self.df)
        unique_count = self.df['text'].nunique()
        uniqueness_ratio = unique_count / total_count
        
        # Якщо унікальних твітів менше 50% — це підозріло (для цього датасету)
        self.assertGreater(uniqueness_ratio, 0.5, f"Занадто низька унікальність даних: {uniqueness_ratio:.2f}")

    def test_text_content_validity(self):
        """Перевірка змісту: текст повинен бути розумної довжини."""
        texts = self.df['text'].astype(str)
        
        # Перевіряємо, чи немає занадто коротких твітів (наприклад, 1 символ)
        too_short = texts[texts.str.len() < 2].count()
        self.assertEqual(too_short, 0, f"Знайдено {too_short} занадто коротких повідомлень.")

if __name__ == "__main__":
    unittest.main()