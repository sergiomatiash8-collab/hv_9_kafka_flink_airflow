from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id='hello_world_check',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task = PythonOperator(
        task_id='say_hello',
        python_callable=lambda: print("Airflow is working!")
    )