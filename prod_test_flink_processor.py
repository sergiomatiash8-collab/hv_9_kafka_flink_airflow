import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Створюємо середовище
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(settings)

# --- ПУНКТ 2 ВИДАЛЕНО: JAR-ФАЙЛИ ПІДТЯГУЮТЬСЯ АВТОМАТИЧНО З /opt/flink/lib/ ---

# 2. Джерело даних (Kafka)
t_env.execute_sql("""
    CREATE TABLE tweets_source (
        author_id STRING,
        created_at STRING,
        `text` STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'tweets',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'flink_group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# 3. Вивід результату (Консоль TaskManager)
t_env.execute_sql("""
    CREATE TABLE tweets_enriched (
        author_id STRING,
        `text` STRING,
        processed_at TIMESTAMP(3),
        enrichment_info STRING
    ) WITH (
        'connector' = 'print'
    )
""")

# 4. Запуск обробки
print("🚀 Надсилаємо джобу у кластер Flink (Local JAR mode)...")
table_result = t_env.execute_sql("""
    INSERT INTO tweets_enriched
    SELECT 
        author_id, 
        `text`, 
        CURRENT_TIMESTAMP, 
        'PROCESSED_BY_FLINK_V1' as enrichment_info
    FROM tweets_source
""")

table_result.wait()