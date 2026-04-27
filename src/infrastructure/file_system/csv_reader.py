import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Union

from ...domain.entities.tweet import Tweet
from ...domain.value_objects.author_id import AuthorId

logger = logging.getLogger(__name__)

class CSVTweetReader:
    """
    Адаптер для роботи з файловою системою.
    Перетворює рядки CSV у сутності Tweet.
    """

    def __init__(self, file_path: Union[str, Path]):
        self._file_path = Path(file_path)
        
        if not self._file_path.exists():
            logger.error(f"Файл не знайдено: {self._file_path}")
            raise FileNotFoundError(f"CSV file not found: {self._file_path}")

    def read_tweets(self) -> List[Tweet]:
        """
        Зчитує CSV та виконує мапінг на доменні об'єкти.
        """
        try:
            # Використовуємо pandas для швидкості та зручності
            df = pd.read_csv(self._file_path, encoding='utf-8')
        except Exception as e:
            logger.error(f"Помилка при читанні CSV: {e}")
            return []

        tweets: List[Tweet] = []
        for index, row in df.iterrows():
            try:
                tweet = self._row_to_tweet(row)
                tweets.append(tweet)
            except Exception as e:
                # Пропускаємо невалідний рядок, але фіксуємо це в логах
                logger.warning(f"Рядок {index} пропущено: {e}")
                continue
        
        return tweets

    def _row_to_tweet(self, row: pd.Series) -> Tweet:
        """Перетворює рядок таблиці (Series) у сутність Tweet."""
        return Tweet(
            author_id=AuthorId(str(row['author_id'])),
            created_at=self._parse_datetime(str(row['created_at'])),
            text=str(row['text'])
        )

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime:
        """Підтримка декількох форматів дати для гнучкості."""
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(dt_str, fmt)
            except ValueError:
                continue
                
        raise ValueError(f"Не вдалося розпізнати формат дати: {dt_str}")