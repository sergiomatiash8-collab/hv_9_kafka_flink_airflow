from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='flink_tweet_enrichment',
    start_date=datetime(2026, 4, 25),
    schedule_interval=None,
    catchup=False,
    tags=['flink', 'kafka']
) as dag:

    # Запускаємо процесор з правильним шляхом до файлу в usrlib
    run_tweet_processor = BashOperator(
        task_id='run_tweet_processor',
        bash_command="""
            docker exec hv_9_kafka_flink_airflow-flink-jobmanager-1 \
            flink run -py /opt/flink/usrlib/03_flink_processor.py
        """
    )

    run_tweet_processor