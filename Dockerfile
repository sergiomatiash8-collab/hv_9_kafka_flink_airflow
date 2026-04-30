# Use the official Airflow image
FROM apache/airflow:2.7.0-python3.10

USER root

# 1. Install Docker CLI
# This allows Airflow to send commands to your Docker host
RUN apt-get update && apt-get install -y \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 2. Install providers and necessary libraries
# The Docker provider is required for DockerOperator
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.7.0 \
    python-dotenv

WORKDIR /opt/airflow