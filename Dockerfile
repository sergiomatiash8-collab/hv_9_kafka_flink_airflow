# Використовуємо офіційний образ Airflow
FROM apache/airflow:2.7.0-python3.10

USER root

# 1. Встановлюємо Docker CLI
# Це дозволить Airflow відправляти команди твоєму Docker-хосту
RUN apt-get update && apt-get install -y \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 2. Встановлюємо провайдери та необхідні бібліотеки
# Провайдер Docker потрібен для DockerOperator (якщо захочеш його використати)
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.7.0 \
    python-dotenv

WORKDIR /opt/airflow