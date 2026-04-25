import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Налаштовуємо середовище
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(settings)

# Вказуємо шлях до Python всередині контейнера
t_env.get_config().set("python.executable", "/usr/bin/python3")

# 2. ПІДКЛЮЧАЄМО КОННЕКТОРИ
# Додаємо Kafka Connector ТА JSON формат (необхідно для парсингу)
kafka_jar = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar"
json_jar = "https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.17.1/flink-json-1.17.1.jar"

t_env.get_config().set("pipeline.jars", f"{kafka_jar};{json_jar}")

# 3. Джерело даних (Kafka)
# Використовуємо внутрішню мережу Docker (kafka:29092)
t_env.execute_sql("""
    CREATE TABLE tweets_source (
        author_id STRING,
        created_at STRING,
        text STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'tweets',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'flink_group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# 4. Вивід результату (Консоль TaskManager)
t_env.execute_sql("""
    CREATE TABLE tweets_enriched (
        author_id STRING,
        text STRING,
        processed_at TIMESTAMP(3),
        enrichment_info STRING
    ) WITH (
        'connector' = 'print'
    )
""")

# 5. Запуск обробки
print("🚀 Надсилаємо джобу у кластер Flink...")
table_result = t_env.execute_sql("""
    INSERT INTO tweets_enriched
    SELECT 
        author_id, 
        text, 
        CURRENT_TIMESTAMP, 
        'PROCESSED_BY_FLINK_V1' as enrichment_info
    FROM tweets_source
""")

# Чекаємо на виконання (стрімінг буде йти вічно, поки не зупиниш)
table_result.wait()