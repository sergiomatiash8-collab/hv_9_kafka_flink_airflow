from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'flink_tweet_enrichment',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Використовуємо docker exec з гнучким пошуком імені контейнера
    # Ми шукаємо контейнер, який має в назві 'flink-jobmanager'
    run_flink_job = BashOperator(
        task_id='run_tweet_processor',
        bash_command="""
            CONTAINER_ID=$(docker ps -aqf "name=flink-jobmanager")
            docker exec $CONTAINER_ID flink run -py /opt/flink/usrlib/03_flink_processor.py
        """
    )

    run_flink_job