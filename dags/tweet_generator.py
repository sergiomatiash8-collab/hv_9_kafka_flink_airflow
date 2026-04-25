from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import random

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    'kafka_tweet_generator',
    default_args=default_args,
    schedule_interval=timedelta(seconds=10),  # Кожні 10 секунд
    catchup=False
) as dag:

    # Генеруємо рандомний твіт і штовхаємо в Kafka
    generate_tweet = BashOperator(
        task_id='send_tweet_to_kafka',
        bash_command="""
        TWEET='{"author_id": "'$((RANDOM%1000))'", "created_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'", "text": "Random tweet #'$((RANDOM%100))'"}'
        docker exec kafka kafka-console-producer --bootstrap-server localhost:9092 --topic tweets <<< "$TWEET"
        """
    )