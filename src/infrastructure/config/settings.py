import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class KafkaSettings(BaseSettings):
    """Kafka configuration loaded from environment."""
    bootstrap_servers: str = Field(default="localhost:9092", validation_alias='KAFKA_BOOTSTRAP_SERVERS')
    topic: str = Field(default='tweets', validation_alias='KAFKA_TOPIC')
    group_id: str = Field(default='tweet-processor', validation_alias='KAFKA_GROUP_ID')

    model_config = SettingsConfigDict(env_prefix='KAFKA_', extra='ignore')

class DatabaseSettings(BaseSettings):
    """PostgreSQL configuration."""
    host: str = Field(default="localhost", validation_alias='POSTGRES_HOST')
    port: int = Field(default=5432, validation_alias='POSTGRES_PORT')
    database: str = Field(default="postgres", validation_alias='POSTGRES_DB')
    username: str = Field(default="postgres", validation_alias='POSTGRES_USER')
    password: str = Field(default="postgres", validation_alias='POSTGRES_PASSWORD')

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    model_config = SettingsConfigDict(env_prefix='POSTGRES_', extra='ignore')

class AppSettings(BaseSettings):
    """Main configuration object (composition)."""
    kafka: KafkaSettings = KafkaSettings()
    database: DatabaseSettings = DatabaseSettings()

    csv_file_path: str = Field(default='data/raw_tweets.csv', validation_alias='CSV_FILE_PATH')
    log_level: str = Field(default='INFO', validation_alias='LOG_LEVEL')

    # .env file configuration
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

# Singleton for application-wide usage
settings = AppSettings()