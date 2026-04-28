import psycopg2
from src.domain.entities.tweet import Tweet
from src.domain.repositories.tweet_repository import ITweetRepository
from src.infrastructure.config.settings import settings

class PostgresTweetRepository(ITweetRepository):
    def __init__(self):
        self.dsn = settings.database.connection_string

    def save(self, tweet: Tweet) -> None:
        sql = """
            INSERT INTO tweets_enriched (author_id, created_at, tweet_text, text_length, company, priority)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (author_id, created_at) DO NOTHING;
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    str(tweet.author_id), 
                    tweet.created_at.isoformat(), 
                    tweet.text, 
                    tweet.text_length, 
                    tweet.company.value if tweet.company else None, 
                    tweet.priority.value if tweet.priority else None
                ))