import os
import logging
from pyflink.table import EnvironmentSettings, TableEnvironment

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_flink_environment():
    """Створення та базове налаштування середовища Flink"""
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)
    
    # Оптимізація та назва джоби
    t_env.get_config().set("parallelism.default", "2")
    t_env.get_config().set("pipeline.name", "TweetEnrichmentPipeline")
    
    # Підключаємо JAR для Kafka (важливо для роботи всередині Docker)
    t_env.get_config().set(
        "pipeline.jars", 
        "file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar"
    )
    return t_env

def create_source_table(t_env):
    """Створення вхідної таблиці Kafka"""
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    
    t_env.execute_sql(f"""
        CREATE TABLE tweets_source (
            author_id STRING,
            created_at STRING,
            `text` STRING,
            event_time TIMESTAMP(3) METADATA FROM 'timestamp',
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'tweets',
            'properties.bootstrap.servers' = '{kafka_servers}',
            'properties.group.id' = 'flink-consumer-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    logger.info("✓ Kafka source table created")

def create_enrichment_logic(t_env):
    """View зі збагаченими даними та партиціюванням для файлів"""
    t_env.execute_sql("""
        CREATE VIEW enriched_tweets AS
        SELECT 
            author_id, 
            created_at, 
            `text`,
            CHAR_LENGTH(`text`) as text_length,
            CASE 
                WHEN author_id = '115852' THEN 'Amazon'
                WHEN author_id = '115854' THEN 'Apple'
                WHEN author_id = '17919972' THEN 'Uber'
                WHEN author_id = '115873' THEN 'Microsoft'
                ELSE 'Other'
            END as company,
            CASE 
                WHEN `text` LIKE '%broken%' OR `text` LIKE '%bad%' OR `text` LIKE '%error%' OR `text` LIKE '%issue%' THEN 'HIGH'
                WHEN `text` LIKE '%help%' OR `text` LIKE '%please%' THEN 'MEDIUM'
                ELSE 'NORMAL'
            END as priority,
            CURRENT_TIMESTAMP as processed_at,
            -- Форматуємо дату для назви папок (CSV)
            DATE_FORMAT(TO_TIMESTAMP(created_at, 'yyyy-MM-dd HH:mm:ss'), 'dd_MM_yyyy_HH_mm') as file_partition
        FROM tweets_source
    """)
    logger.info("✓ Enrichment view created")

def create_postgres_sink(t_env):
    """Таблиця-приймач для PostgreSQL"""
    host = os.getenv("POSTGRES_HOST", "postgres")
    db = os.getenv("POSTGRES_DB", "tweets_db")
    user = os.getenv("POSTGRES_USER", "admin")
    pw = os.getenv("POSTGRES_PASSWORD", "admin123")
    
    jdbc_url = f"jdbc:postgresql://{host}:5432/{db}"
    
    t_env.execute_sql(f"""
        CREATE TABLE postgres_sink (
            author_id STRING,
            created_at STRING,
            tweet_text STRING,
            text_length INT,
            company STRING,
            priority STRING,
            processed_at TIMESTAMP(3),
            PRIMARY KEY (author_id, created_at) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{jdbc_url}',
            'table-name' = 'tweets_enriched',
            'username' = '{user}',
            'password' = '{pw}',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    logger.info("✓ PostgreSQL sink created")

def create_file_sink(t_env):
    """Таблиця-приймач для CSV файлів"""
    path = os.getenv("OUTPUT_DIR", "/opt/flink/usrlib/output")
    
    t_env.execute_sql(f"""
        CREATE TABLE file_sink (
            file_partition STRING,
            author_id STRING,
            created_at STRING,
            tweet_text STRING,
            company STRING,
            priority STRING
        ) PARTITIONED BY (file_partition)
        WITH (
            'connector' = 'filesystem',
            'path' = '{path}',
            'format' = 'csv',
            'sink.partition-commit.policy.kind' = 'success-file'
        )
    """)
    logger.info("✓ File sink created")

def main():
    logger.info("Starting Tweet Enrichment Pipeline...")
    try:
        t_env = create_flink_environment()
        create_source_table(t_env)
        create_enrichment_logic(t_env)
        create_postgres_sink(t_env)
        create_file_sink(t_env)

        # Запускаємо два інсерти одночасно
        statement_set = t_env.create_statement_set()
        
        statement_set.add_insert_sql("INSERT INTO postgres_sink SELECT author_id, created_at, `text`, text_length, company, priority, processed_at FROM enriched_tweets")
        statement_set.add_insert_sql("INSERT INTO file_sink SELECT file_partition, author_id, created_at, `text`, company, priority FROM enriched_tweets")
        
        table_result = statement_set.execute()
        logger.info("🚀 Pipeline is running. Writing to DB and CSV...")
        table_result.wait()

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()