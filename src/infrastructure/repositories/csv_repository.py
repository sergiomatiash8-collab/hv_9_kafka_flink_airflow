"""
Repository для збереження твітів у CSV файли.

Правила:
    - Формат назви: tweets_dd_mm_yyyy_hh_mm.csv
    - Розподіл: по хвилинах (кожна хвилина = новий файл)
    - Encoding: UTF-8
    - Headers: автоматично додаються для нових файлів

Clean Architecture шар: Infrastructure Layer
"""

import csv
import os
from datetime import datetime
from src.domain.entities.tweet import Tweet
import logging

logger = logging.getLogger(__name__)

class CSVTweetRepository:
    """
    Repository для збереження Tweet entities у CSV формат.
    
    Відповідає за:
        - Форматування назв файлів
        - Створення директорій
        - Append дані до існуючих файлів
        - Запис заголовків для нових файлів
    """
    
    def __init__(self, base_path: str = "output"):
        """
        Ініціалізація repository.
        
        Args:
            base_path: Базова директорія для збереження CSV файлів
        """
        self.base_path = base_path
        self._ensure_directory_exists()
        logger.info(f"📁 CSV Repository ініціалізовано: {os.path.abspath(base_path)}")
    
    def _ensure_directory_exists(self) -> None:
        """Створення директорії якщо не існує."""
        os.makedirs(self.base_path, exist_ok=True)
    
    def save(self, tweet: Tweet) -> None:
        """
        Збереження твіту у CSV файл.
        
        Args:
            tweet: Domain Entity з даними твіту
            
        Raises:
            Exception: у разі помилки запису
        """
        filepath = self._get_current_filepath()
        file_exists = os.path.isfile(filepath)
        
        try:
            with open(filepath, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Запис заголовків для нових файлів
                if not file_exists:
                    self._write_headers(writer)
                    logger.info(f"📄 Створено новий файл: {os.path.basename(filepath)}")
                
                # Запис даних твіту
                self._write_tweet_row(writer, tweet)
                
        except Exception as e:
            logger.error(f"❌ Помилка запису в CSV {filepath}: {e}")
            raise
    
    def _get_current_filepath(self) -> str:
        """
        Формування шляху до поточного CSV файлу.
        
        Returns:
            Повний шлях до файлу у форматі: tweets_dd_mm_yyyy_hh_mm.csv
            
        Приклад:
            tweets_27_10_2024_14_30.csv
        """
        # ✅ ВИПРАВЛЕНО: правильний формат (день_місяць_рік_година_хвилина)
        filename = f"tweets_{datetime.now().strftime('%d_%m_%Y_%H_%M')}.csv"
        return os.path.join(self.base_path, filename)
    
    def _write_headers(self, writer: csv.writer) -> None:
        """
        Запис заголовків CSV файлу.
        
        Args:
            writer: CSV writer об'єкт
        """
        headers = [
            'author_id',
            'created_at',
            'text',
            'text_length',
            'company',
            'priority',
            'processed_at'
        ]
        writer.writerow(headers)
    
    def _write_tweet_row(self, writer: csv.writer, tweet: Tweet) -> None:
        """
        Запис даних твіту у CSV.
        
        Args:
            writer: CSV writer об'єкт
            tweet: Tweet entity
        """
        # Безпечна екстракція значень з Value Objects
        author_id = self._extract_value(tweet.author_id)
        company = self._extract_value(tweet.company)
        priority = self._extract_value(tweet.priority)
        
        row = [
            author_id,
            tweet.created_at.isoformat(),
            tweet.text,
            len(tweet.text),  # text_length
            company,
            priority,
            datetime.now().isoformat()  # processed_at
        ]
        
        writer.writerow(row)
    
    def _extract_value(self, obj) -> str:
        """
        Безпечна екстракція значення з Value Object.
        
        Args:
            obj: Value Object або None
            
        Returns:
            Значення у вигляді рядка або порожній рядок
        """
        if obj is None:
            return ''
        
        # Якщо є атрибут value (Value Object)
        if hasattr(obj, 'value'):
            return str(obj.value)
        
        # Інакше просто конвертуємо в рядок
        return str(obj)
    
    def get_files_count(self) -> int:
        """
        Підрахунок кількості створених CSV файлів.
        
        Returns:
            Кількість файлів формату tweets_*.csv
        """
        files = [f for f in os.listdir(self.base_path) if f.startswith('tweets_') and f.endswith('.csv')]
        return len(files)