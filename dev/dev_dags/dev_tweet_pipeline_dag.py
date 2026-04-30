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
    """Check Kafka port availability inside Docker network"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            # Use internal port 29092
            result = s.connect_ex(('kafka', 29092))
            return result == 0
    except Exception as e:
        print(f"Kafka check failed: {e}")
        return False

def start_producer():
    """Start producer container if it is not running"""
    try:
        # Check container status
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
    """Submit Python script for execution in Flink JobManager"""
    try:
        # Find JobManager container by service name
        find_id = ["docker", "ps", "-qf", "name=flink-jobmanager"]
        container_id = subprocess.check_output(find_id).decode().strip()

        if not container_id:
            raise Exception("Flink JobManager container not found")

        # Run command (path must match docker-compose volume)
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
    """Check if CSV files exist in output directory"""
    output_dir = "/opt/flink/usrlib/output"
    if not os.path.exists(output_dir):
        print("Output directory does not exist yet")
        return False

    # Search for CSV files in partition subfolders
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
    schedule_interval=None,  # Manual запуск for testing
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['production', 'streaming'],
) as dag:

    # 1. Wait until Kafka is available
    t1 = PythonSensor(
        task_id='wait_for_kafka',
        python_callable=check_kafka_ready,
        timeout=300,
        poke_interval=15
    )

    # 2. Start tweet generator
    t2 = PythonOperator(
        task_id='start_producer',
        python_callable=start_producer
    )

    # 3. Short pause for initial data accumulation
    t3 = BashOperator(
        task_id='wait_for_initial_batch',
        bash_command='sleep 20'
    )

    # 4. Deploy Flink job
    t4 = PythonOperator(
        task_id='submit_flink_job',
        python_callable=submit_flink_job
    )

    # 5. Let the system run for 5 minutes
    t5 = BashOperator(
        task_id='monitor_streaming',
        bash_command='sleep 300'
    )

    # 6. Validate output files
    t6 = PythonOperator(
        task_id='validate_results',
        python_callable=validate_output_files
    )

    # Execution order
    t1 >> t2 >> t3 >> t4 >> t5 >> t6