import logging
from pyflink.table import EnvironmentSettings, TableEnvironment

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_tweet_enrichment_processor():
    """
    Main Flink Job function for tweet enrichment.

    Process:
    1. Reads RAW data from 'tweets' topic
    2. Applies enrichment business logic:
       - Adds processed_at (processing time)
       - Computes text_length
       - Determines company (based on author_id % 4)
       - Determines priority (based on text_length)
    3. Writes enriched data to 'tweets_enriched' topic
    """

    logger.info("=" * 70)
    logger.info("FLINK TWEET ENRICHMENT PROCESSOR - STARTING")
    logger.info("=" * 70)

    # 1. Create streaming environment
    logger.info("Step 1: Creating Flink StreamTableEnvironment...")
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)

    # Error handling configuration
    t_env.get_config().set("table.exec.sink.not-null-enforcer", "DROP")
    t_env.get_config().set("pipeline.name", "TweetEnrichmentPipeline")

    logger.info("Flink environment ready")

    # 2. Create source table (Kafka input)
    logger.info("Step 2: Creating source table for 'tweets' topic...")

    t_env.execute_sql("""
        CREATE TABLE tweets_source (
            author_id STRING,
            created_at STRING,
            text STRING,
            event_time AS TO_TIMESTAMP(created_at),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'tweets',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-enrichment-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.fail-on-missing-field' = 'false'
        )
    """)

    logger.info("Source table created (topic: tweets)")

    # 3. Create sink table (Kafka output)
    logger.info("Step 3: Creating sink table for 'tweets_enriched' topic...")

    t_env.execute_sql("""
        CREATE TABLE tweets_enriched_sink (
            author_id STRING,
            created_at STRING,
            text STRING,
            text_length INT,
            processed_at STRING,
            company STRING,
            priority STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'tweets_enriched',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false'
        )
    """)

    logger.info("Sink table created (topic: tweets_enriched)")

    # 4. Enrichment logic
    logger.info("Step 4: Running enrichment pipeline...")

    enrichment_query = """
        INSERT INTO tweets_enriched_sink
        SELECT
            author_id,
            created_at,
            text,

            CHAR_LENGTH(text) as text_length,

            CAST(CURRENT_TIMESTAMP AS STRING) as processed_at,

            CASE
                WHEN MOD(CAST(author_id AS BIGINT), 4) = 0 THEN 'Apple'
                WHEN MOD(CAST(author_id AS BIGINT), 4) = 1 THEN 'Google'
                WHEN MOD(CAST(author_id AS BIGINT), 4) = 2 THEN 'Microsoft'
                ELSE 'Amazon'
            END as company,

            CASE
                WHEN CHAR_LENGTH(text) > 200 THEN 'HIGH'
                WHEN CHAR_LENGTH(text) > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END as priority

        FROM tweets_source
        WHERE
            author_id IS NOT NULL
            AND text IS NOT NULL
            AND text <> ''
            AND CHAR_LENGTH(text) > 0
    """

    table_result = t_env.execute_sql(enrichment_query)

    logger.info("=" * 70)
    logger.info("FLINK JOB STARTED SUCCESSFULLY")
    logger.info("=" * 70)
    logger.info("Source: tweets")
    logger.info("Sink: tweets_enriched")
    logger.info("Enrichment: text_length, processed_at, company, priority")
    logger.info("=" * 70)
    logger.info("Flink Dashboard: http://localhost:8081")
    logger.info("=" * 70)

    # Keep streaming job running
    table_result.wait()

if __name__ == '__main__':
    try:
        run_tweet_enrichment_processor()
    except KeyboardInterrupt:
        logger.info("Stop signal received (CTRL+C)")
    except Exception as e:
        logger.error(f"Critical Flink job error: {e}", exc_info=True)
        raise