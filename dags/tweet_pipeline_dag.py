import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
import socket

# Налаштування за замовчуванням
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_kafka_ready():
    """Перевірка доступності брокера всередині docker-network"""
    try:
        with socket.create_connection(("kafka", 29092), timeout=5):
            return True
    except OSError:
        return False

with DAG(
    'twitter_pipeline_full_cycle',
    default_args=default_args,
    description='Пайплайн: Producer -> Flink -> Clean Consumer',
    schedule_interval=None, # Запуск вручну
    start_date=datetime(2023, 10, 27),
    catchup=False,
    tags=['twitter', 'clean_arch', 'kafka', 'flink'],
) as dag:

    # 1. Перевірка інфраструктури
    wait_for_kafka = PythonOperator(
        task_id='wait_for_kafka',
        python_callable=check_kafka_ready
    )

    # 2. Запуск Продюсера (завантаження raw_tweets.csv в топік 'tweets')
    run_producer = DockerOperator(
        task_id='run_producer',
        image='twitter_pipeline-producer:latest',
        container_name='airflow_exec_producer',
        api_version='auto',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="twitter-pipeline_kafka-network",
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
            'KAFKA_TOPIC': 'tweets'
        }
    )

    # 3. Сабміт Flink Job (обробка та збагачення даних)
    # Команда flink run запускається всередині існуючого JobManager
    submit_flink_job = DockerOperator(
        task_id='submit_flink_job',
        image='twitter_pipeline-flink-jobmanager:latest',
        container_name='airflow_exec_flink',
        api_version='auto',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="twitter-pipeline_kafka-network",
        command="flink run -py /opt/flink/usrlib/flink_consumer.py",
        environment={
            'FLINK_PROPERTIES': "jobmanager.rpc.address: flink-jobmanager"
        }
    )

    # 4. Запуск Clean Arch Worker (Запис збагачених даних у DB та CSV)
    run_worker = DockerOperator(
        task_id='run_final_worker',
        image='twitter_pipeline-worker-consumer:latest',
        container_name='airflow_exec_worker',
        api_version='auto',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="twitter-pipeline_kafka-network",
        # Дані пишуться в змонтований volume 'output' через Dockerfile
    )

    # Логічний ланцюг виконання
    wait_for_kafka >> run_producer >> submit_flink_job >> run_worker