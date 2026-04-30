import csv
import os
from datetime import datetime
from src.domain.entities.tweet import Tweet
import logging

logger = logging.getLogger(__name__)

class CSVTweetRepository:
    """
    Repository for saving Tweet entities into CSV files.

    Rules:
        - Filename format: tweets_dd_mm_yyyy_hh_mm.csv
        - Partitioning: by minute (each minute = new file)
        - Encoding: UTF-8
        - Headers: automatically added for new files

    Infrastructure layer component.
    """

    def __init__(self, base_path: str = "output"):
        """
        Initialize repository.

        Args:
            base_path: Base directory for CSV storage
        """
        self.base_path = base_path
        self._ensure_directory_exists()
        logger.info(f"CSV Repository initialized: {os.path.abspath(base_path)}")

    def _ensure_directory_exists(self) -> None:
        """Create directory if it does not exist."""
        os.makedirs(self.base_path, exist_ok=True)

    def save(self, tweet: Tweet) -> None:
        """
        Save tweet into CSV file.

        Args:
            tweet: Tweet domain entity

        Raises:
            Exception: if write operation fails
        """
        filepath = self._get_current_filepath()
        file_exists = os.path.isfile(filepath)

        try:
            with open(filepath, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)

                # Write headers for new files
                if not file_exists:
                    self._write_headers(writer)
                    logger.info(f"Created new file: {os.path.basename(filepath)}")

                # Write tweet row
                self._write_tweet_row(writer, tweet)

        except Exception as e:
            logger.error(f"CSV write error {filepath}: {e}")
            raise

    def _get_current_filepath(self) -> str:
        """
        Build current CSV file path.

        Returns:
            Full path in format: tweets_dd_mm_yyyy_hh_mm.csv
        """
        filename = f"tweets_{datetime.now().strftime('%d_%m_%Y_%H_%M')}.csv"
        return os.path.join(self.base_path, filename)

    def _write_headers(self, writer: csv.writer) -> None:
        """
        Write CSV headers.

        Args:
            writer: CSV writer instance
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
        Write tweet data row.

        Args:
            writer: CSV writer instance
            tweet: Tweet entity
        """
        author_id = self._extract_value(tweet.author_id)
        company = self._extract_value(tweet.company)
        priority = self._extract_value(tweet.priority)

        row = [
            author_id,
            tweet.created_at.isoformat(),
            tweet.text,
            len(tweet.text),
            company,
            priority,
            datetime.now().isoformat()
        ]

        writer.writerow(row)

    def _extract_value(self, obj) -> str:
        """
        Safely extract value from Value Object.

        Args:
            obj: Value object or None

        Returns:
            String representation or empty string
        """
        if obj is None:
            return ""

        if hasattr(obj, "value"):
            return str(obj.value)

        return str(obj)

    def get_files_count(self) -> int:
        """
        Count created CSV files.

        Returns:
            Number of tweets_*.csv files
        """
        files = [
            f for f in os.listdir(self.base_path)
            if f.startswith("tweets_") and f.endswith(".csv")
        ]
        return len(files)