FROM apache/airflow:2.7.0-python3.10

USER root

RUN apt-get update && apt-get install -y docker.io && apt-get clean


USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.7.0 \
    python-dotenv \
    kafka-python \
    pydantic-settings


USER root
RUN chown -R airflow: /opt/airflow

USER airflow
WORKDIR /opt/airflow