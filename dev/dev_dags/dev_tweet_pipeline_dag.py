from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
import subprocess
import os
import socket

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

def check_kafka_ready():
    """Перевірка доступності порту Kafka всередині мережі Docker"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            # Використовуємо внутрішній порт 29092
            result = s.connect_ex(('kafka', 29092))
            return result == 0
    except Exception as e:
        print(f"Kafka check failed: {e}")
        return False

def start_producer():
    """Запуск контейнера-продюсера, якщо він не запущений"""
    try:
        # Перевіряємо статус
        check_cmd = ["docker", "inspect", "-f", "{{.State.Running}}", "producer"]
        result = subprocess.run(check_cmd, capture_output=True, text=True)
        
        if "true" not in result.stdout.lower():
            print("Starting producer container...")
            subprocess.run(["docker", "start", "producer"], check=True)
        else:
            print("Producer is already running")
        return True
    except Exception as e:
        print(f"Failed to manage producer: {e}")
        return False

def submit_flink_job():
    """Відправка Python-скрипта на виконання у Flink JobManager"""
    try:
        # Шукаємо контейнер JobManager за назвою сервісу
        find_id = ["docker", "ps", "-qf", "name=flink-jobmanager"]
        container_id = subprocess.check_output(find_id).decode().strip()
        
        if not container_id:
            raise Exception("Flink JobManager container not found")

        # Команда запуску (шлях має збігатися з волюмом у docker-compose)
        submit_cmd = [
            "docker", "exec", container_id,
            "flink", "run", "-d", "-py", "/opt/flink/usrlib/03_flink_processor.py"
        ]
        subprocess.run(submit_cmd, check=True)
        return True
    except Exception as e:
        print(f"Flink submission error: {e}")
        return False

def validate_output_files():
    """Перевірка наявності CSV файлів у папці output"""
    output_dir = "/opt/flink/usrlib/output"
    if not os.path.exists(output_dir):
        print("Output directory does not exist yet")
        return False
    
    # Шукаємо CSV у підпапках партицій
    csv_files = []
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    
    print(f"Found {len(csv_files)} output files.")
    return len(csv_files) > 0

with DAG(
    'tweet_enrichment_pipeline',
    default_args=default_args,
    description='End-to-End: Kafka -> Flink -> Postgres/CSV',
    schedule_interval=None, # Запуск вручну для тестів
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['production', 'streaming'],
) as dag:

    # 1. Чекаємо, поки підніметься Kafka
    t1 = PythonSensor(
        task_id='wait_for_kafka',
        python_callable=check_kafka_ready,
        timeout=300,
        poke_interval=15
    )

    # 2. Стартуємо генератор твітів
    t2 = PythonOperator(
        task_id='start_producer',
        python_callable=start_producer
    )

    # 3. Невелика пауза для накопичення перших даних
    t3 = BashOperator(
        task_id='wait_for_initial_batch',
        bash_command='sleep 20'
    )

    # 4. Деплоїмо джобу у Flink
    t4 = PythonOperator(
        task_id='submit_flink_job',
        python_callable=submit_flink_job
    )

    # 5. Даємо системі попрацювати 5 хвилин
    t5 = BashOperator(
        task_id='monitor_streaming',
        bash_command='sleep 300'
    )

    # 6. Перевіряємо, чи є результат на диску
    t6 = PythonOperator(
        task_id='validate_results',
        python_callable=validate_output_files
    )

    # Послідовність виконання
    t1 >> t2 >> t3 >> t4 >> t5 >> t6