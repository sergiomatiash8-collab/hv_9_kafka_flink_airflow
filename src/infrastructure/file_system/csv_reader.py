import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Union

from ...domain.entities.tweet import Tweet
from ...domain.value_objects.author_id import AuthorId
from ...domain.value_objects.company import Company

logger = logging.getLogger(__name__)

class CSVTweetReader:
    """
    Адаптер для роботи з файловою системою.
    Перетворює рядки CSV у сутності Tweet з повною валідацією.
    """

    def __init__(self, file_path: Union[str, Path]):
        self._file_path = Path(file_path)
        
        if not self._file_path.exists():
            logger.error(f"Файл не знайдено: {self._file_path}")
            raise FileNotFoundError(f"CSV файл не знайдено за шляхом: {self._file_path}")

    def read_tweets(self) -> List[Tweet]:
        """
        Зчитує CSV та виконує мапінг на доменні об'єкти.
        """
        try:
            # Читаємо файл. Платформа Kaggle часто дає специфічне кодування, utf-8 зазвичай ок.
            df = pd.read_csv(self._file_path, encoding='utf-8')
        except Exception as e:
            logger.error(f"Критична помилка при читанні CSV: {e}")
            return []

        tweets: List[Tweet] = []
        for index, row in df.iterrows():
            try:
                tweet = self._row_to_tweet(row)
                tweets.append(tweet)
            except Exception as e:
                # Це дозволяє не "валити" весь процес через один кривий рядок
                logger.warning(f"Рядок {index} пропущено через помилку: {e}")
                continue
        
        logger.info(f"Успішно зчитано {len(tweets)} об'єктів Tweet")
        return tweets

    def _row_to_tweet(self, row: pd.Series) -> Tweet:
        """
        Перетворює рядок таблиці у валідну сутність Tweet.
        Використовує бізнес-логіку для визначення компанії.
        """
        raw_author_id = str(row['author_id'])
        
        # Використовуємо твою логіку автоматичного визначення компанії
        company = Company.from_author_id(raw_author_id)
        
        return Tweet(
            author_id=AuthorId(raw_author_id),
            created_at=self._parse_datetime(str(row['created_at'])),
            text=str(row['text']),
            company=company
            # priority виставиться в None за замовчуванням або додасться пізніше сервісом
        )

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime:
        """
        Парсить дату, враховуючи специфічний формат Kaggle Twitter Dataset.
        """
        formats = [
            '%a %b %d %H:%M:%S %z %Y',  # Формат Kaggle: Wed Oct 11 06:55:44 +0000 2017
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(dt_str, fmt)
            except ValueError:
                continue
                
        raise ValueError(f"Невідомий формат дати: {dt_str}")