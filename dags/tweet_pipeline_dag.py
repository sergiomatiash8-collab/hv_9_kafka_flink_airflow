"""
Airflow DAG для оркестрації повного пайплайну обробки твітів.

Архітектура потоку:
    1. Перевірка Kafka доступності
    2. Створення необхідних топіків
    3. Запуск Producer (raw_tweets.csv → Kafka 'tweets')
    4. Запуск Flink Job (збагачення: 'tweets' → 'tweets_enriched')
    5. Запуск Consumer (збагачені дані → PostgreSQL + CSV файли)

Автор: Data Engineering Team
Дата створення: 2024-10-27
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import socket
import logging

logger = logging.getLogger(__name__)

# ==================== КОНФІГУРАЦІЯ ====================

# Назви образів (автоматично формуються Docker Compose)
PROJECT_NAME = "hv_9_kafka_flink_airflow"
PRODUCER_IMAGE = f"{PROJECT_NAME}-producer:latest"
CONSUMER_IMAGE = f"{PROJECT_NAME}-worker-consumer:latest"
NETWORK_MODE = f"{PROJECT_NAME}_kafka-network"

# Налаштування Kafka
KAFKA_BOOTSTRAP_SERVERS_INTERNAL = "kafka:29092"
KAFKA_BOOTSTRAP_SERVERS_EXTERNAL = "localhost:9092"
TOPIC_RAW = "tweets"
TOPIC_ENRICHED = "tweets_enriched"

# ==================== DEFAULT ARGUMENTS ====================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

# ==================== HELPER FUNCTIONS ====================

def check_kafka_ready(**context):
    """
    Перевірка доступності Kafka брокера.
    
    Намагається встановити TCP з'єднання з Kafka.
    Якщо не вдається - викидає exception для retry.
    """
    logger.info("🔍 Перевірка доступності Kafka...")
    
    try:
        with socket.create_connection(("kafka", 29092), timeout=10):
            logger.info("✅ Kafka доступна і готова до роботи!")
            return True
    except OSError as e:
        logger.error(f"❌ Kafka недоступна: {e}")
        raise Exception(f"Kafka broker недоступний: {e}")

# ==================== DAG DEFINITION ====================

with DAG(
    dag_id='tweet_enrichment_pipeline_v2',
    default_args=default_args,
    description='Повний цикл обробки твітів: Producer → Flink (збагачення) → Consumer → CSV/DB',
    schedule_interval=None,  # Тригер - ручний запуск
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka', 'flink', 'airflow', 'tweets', 'streaming', 'enrichment'],
    doc_md=__doc__,
) as dag:

    # ==================== TASK 1: ПЕРЕВІРКА KAFKA ====================
    
    check_kafka_availability = PythonOperator(
        task_id='check_kafka_availability',
        python_callable=check_kafka_ready,
        provide_context=True,
        doc_md="""
        ### Перевірка Kafka
        Встановлює TCP з'єднання з Kafka брокером.
        У разі недоступності - retry після 2 хвилин.
        """
    )
    
    # ==================== TASK 2: СТВОРЕННЯ ТОПІКІВ ====================
    
    create_kafka_topics = BashOperator(
        task_id='create_kafka_topics',
        bash_command=f"""
            # Створення топіку для RAW даних
            docker exec kafka kafka-topics --create --if-not-exists \
                --bootstrap-server {KAFKA_BOOTSTRAP_SERVERS_EXTERNAL} \
                --topic {TOPIC_RAW} \
                --partitions 3 \
                --replication-factor 1 \
                --config retention.ms=86400000 || true
            
            echo "✅ Топік '{TOPIC_RAW}' створено або вже існує"
            
            # Створення топіку для ЗБАГАЧЕНИХ даних
            docker exec kafka kafka-topics --create --if-not-exists \
                --bootstrap-server {KAFKA_BOOTSTRAP_SERVERS_EXTERNAL} \
                --topic {TOPIC_ENRICHED} \
                --partitions 3 \
                --replication-factor 1 \
                --config retention.ms=86400000 || true
            
            echo "✅ Топік '{TOPIC_ENRICHED}' створено або вже існує"
            
            # Перевірка створених топіків
            docker exec kafka kafka-topics --list \
                --bootstrap-server {KAFKA_BOOTSTRAP_SERVERS_EXTERNAL}
        """,
        doc_md="""
        ### Створення Kafka топіків
        - **tweets**: для RAW даних з Producer
        - **tweets_enriched**: для збагачених даних з Flink
        
        Параметри:
        - Partitions: 3 (для паралельної обробки)
        - Replication: 1 (для dev середовища)
        - Retention: 24 години
        """
    )
    
    # ==================== TASK 3: ЗАПУСК PRODUCER ====================
    
    run_kafka_producer = DockerOperator(
        task_id='run_kafka_producer',
        image=PRODUCER_IMAGE,
        container_name='airflow_producer_{{ ts_nodash }}',
        api_version='auto',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK_MODE,
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': KAFKA_BOOTSTRAP_SERVERS_INTERNAL,
            'KAFKA_TOPIC': TOPIC_RAW,
            'CSV_FILE': '/app/raw_tweets.csv',
            'MESSAGES_PER_SECOND': '5',  # Швидкість відправки
        },
        mount_tmp_dir=False,
        tty=True,
        doc_md="""
        ### Kafka Producer
        Читає CSV файл з raw даними та відправляє їх у топік 'tweets'.
        
        Формат повідомлення:
```json
        {
            "author_id": "123456",
            "created_at": "2024-10-27T14:30:00",
            "text": "Sample tweet text..."
        }
```
        """
    )
    
    # ==================== TASK 4: ЗАПУСК FLINK JOB ====================
    
    submit_flink_enrichment_job = BashOperator(
        task_id='submit_flink_enrichment_job',
        bash_command=f"""
            echo "🚀 Запуск Flink Job для збагачення даних..."
            
            # Запуск Python Flink Job всередині JobManager контейнера
            docker exec flink-jobmanager \
                flink run \
                --python /opt/flink/usrlib/tweet_enrichment_processor.py \
                --jobmanager flink-jobmanager:8081 \
                --detached
            
            echo "✅ Flink Job запущена в detached режимі"
            echo "💡 Переглянути статус: http://localhost:8081"
            
            # Чекаємо 10 секунд для старту Job
            sleep 10
        """,
        doc_md="""
        ### Flink Enrichment Job
        Запускає Apache Flink Job для збагачення потоку даних.
        
        Збагачення включає:
        - **text_length**: довжина тексту
        - **processed_at**: timestamp обробки
        - **company**: визначення компанії (Apple/Google/Microsoft/Amazon)
        - **priority**: пріоритет на основі довжини (HIGH/MEDIUM/LOW)
        
        Input: топік 'tweets'
        Output: топік 'tweets_enriched'
        """
    )
    
    # ==================== TASK 5: ЗАПУСК CONSUMER ====================
    
    run_final_consumer = DockerOperator(
        task_id='run_final_consumer',
        image=CONSUMER_IMAGE,
        container_name='airflow_consumer_{{ ts_nodash }}',
        api_version='auto',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK_MODE,
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': KAFKA_BOOTSTRAP_SERVERS_INTERNAL,
            'KAFKA_TOPIC': TOPIC_ENRICHED,  # ✅ Читає ЗБАГАЧЕНІ дані
            'POSTGRES_HOST': 'postgres',
            'POSTGRES_PORT': '5432',
            'POSTGRES_USER': 'admin',
            'POSTGRES_PASSWORD': 'admin123',
            'POSTGRES_DB': 'tweets_db',
            'CSV_OUTPUT_DIR': '/app/output',
        },
        mount_tmp_dir=False,
        tty=True,
        execution_timeout=timedelta(minutes=15),
        doc_md="""
        ### Final Consumer
        Споживає збагачені дані з топіку 'tweets_enriched' та зберігає їх:
        
        1. **PostgreSQL**: у таблицю `tweets_enriched`
        2. **CSV файли**: формат `tweets_dd_mm_yyyy_hh_mm.csv`
        
        Розподіл CSV: по хвилинах (кожна хвилина = новий файл)
        """
    )
    
    # ==================== ВИЗНАЧЕННЯ ПОСЛІДОВНОСТІ ====================
    
    """
    Граф виконання:
    
    check_kafka_availability 
        ↓
    create_kafka_topics
        ↓
    run_kafka_producer
        ↓
    submit_flink_enrichment_job
        ↓
    run_final_consumer
    """
    
    (
        check_kafka_availability 
        >> create_kafka_topics 
        >> run_kafka_producer 
        >> submit_flink_enrichment_job 
        >> run_final_consumer
    )