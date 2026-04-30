import logging
from pyflink.table import EnvironmentSettings, TableEnvironment

# Logging configuration for Docker console tracking
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_tweet_processor():
    logger.info("Starting ULTIMATE version of processor...")

    # Create streaming environment
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)

    # Configuration: DROP on NULL insert into NOT NULL column
    t_env.get_config().set("table.exec.sink.not-null-enforcer", "DROP")

    # 1. Source (Kafka) with JSON error tolerance
    t_env.execute_sql("""
        CREATE TABLE tweets_source (
            tweet_id STRING,
            text STRING,
            author_id STRING,
            created_at TIMESTAMP(3),
            WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'tweets',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.fail-on-missing-field' = 'false'
        )
    """)
    logger.info("Source table ready (with JSON error tolerance)")

    # 2. Sink (PostgreSQL)
    t_env.execute_sql("""
        CREATE TABLE postgres_sink (
            tweet_id STRING,
            text STRING,
            author_id STRING,
            created_at TIMESTAMP(3),
            PRIMARY KEY (tweet_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/tweets_db',
            'table-name' = 'processed_tweets',
            'username' = 'user',
            'password' = 'password',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    logger.info("Sink table ready")

    # 3. Insert with validation filters
    t_env.execute_sql("""
        INSERT INTO postgres_sink
        SELECT tweet_id, text, author_id, created_at
        FROM tweets_source
        WHERE tweet_id IS NOT NULL
          AND author_id IS NOT NULL
          AND text IS NOT NULL
    """)

    logger.info("Job submitted successfully. Now ignoring invalid records.")

if __name__ == '__main__':
    run_tweet_processor()