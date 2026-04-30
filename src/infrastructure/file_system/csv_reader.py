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
    File system adapter.
    Converts CSV rows into Tweet domain entities with full validation.
    """

    def __init__(self, file_path: Union[str, Path]):
        self._file_path = Path(file_path)

        if not self._file_path.exists():
            logger.error(f"File not found: {self._file_path}")
            raise FileNotFoundError(f"CSV file not found at path: {self._file_path}")

    def read_tweets(self) -> List[Tweet]:
        """
        Reads CSV and maps it to domain objects.
        """
        try:
            df = pd.read_csv(self._file_path, encoding='utf-8')
        except Exception as e:
            logger.error(f"Critical error while reading CSV: {e}")
            return []

        tweets: List[Tweet] = []
        for index, row in df.iterrows():
            try:
                tweet = self._row_to_tweet(row)
                tweets.append(tweet)
            except Exception as e:
                # Prevents full pipeline failure due to a single bad row
                logger.warning(f"Row {index} skipped due to error: {e}")
                continue

        logger.info(f"Successfully loaded {len(tweets)} Tweet objects")
        return tweets

    def _row_to_tweet(self, row: pd.Series) -> Tweet:
        """
        Converts a dataframe row into a valid Tweet entity.
        Uses business logic for company detection.
        """
        raw_author_id = str(row['author_id'])

        company = Company.from_author_id(raw_author_id)

        return Tweet(
            author_id=AuthorId(raw_author_id),
            created_at=self._parse_datetime(str(row['created_at'])),
            text=str(row['text']),
            company=company
        )

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime:
        """
        Parses datetime using Kaggle Twitter dataset formats.
        """
        formats = [
            '%a %b %d %H:%M:%S %z %Y',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(dt_str, fmt)
            except ValueError:
                continue

        raise ValueError(f"Unknown datetime format: {dt_str}")