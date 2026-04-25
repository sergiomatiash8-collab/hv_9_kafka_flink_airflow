import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Налаштування
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(settings)
t_env.get_config().set("parallelism.default", "1")

# 2. Джерело (Kafka)
t_env.execute_sql("""
    CREATE TABLE tweets_source (
        author_id STRING,
        created_at STRING,
        `text` STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'tweets',
        'properties.bootstrap.servers' = '172.20.0.7:9092',
        'properties.group.id' = 'flink_enrichment_v3',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# 3. Вивід (Консоль)
t_env.execute_sql("""
    CREATE TABLE tweets_enriched (
        author_id STRING,
        `text` STRING,
        processed_at TIMESTAMP(3),
        enrichment_info STRING
    ) WITH ( 'connector' = 'print' )
""")

print("🚀 Запуск стримінгу (чекаю на дані)...")

# 4. Логіка
table_result = t_env.execute_sql("""
    INSERT INTO tweets_enriched
    SELECT 
        author_id, 
        `text`, 
        CURRENT_TIMESTAMP, 
        'COMP: ' || (CASE 
            WHEN author_id = '115852' THEN 'Amazon'
            WHEN author_id = '115854' THEN 'Apple'
            ELSE 'Other'
        END) || 
        ' | PRIORITY: ' || (CASE 
            WHEN `text` LIKE '%broken%' OR `text` LIKE '%bad%' OR `text` LIKE '%error%' THEN 'HIGH'
            ELSE 'NORMAL'
        END)
    FROM tweets_source
""")

# Правильний спосіб тримати скрипт живим до ручної зупинки
table_result.get_job_client().get_job_execution_result().result()