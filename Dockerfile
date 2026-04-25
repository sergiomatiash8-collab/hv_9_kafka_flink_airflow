FROM apache/airflow:2.7.1
USER root
# Встановлюємо docker клієнт, щоб Airflow міг керувати іншими контейнерами
RUN apt-get update && apt-get install -y docker.io && apt-get clean
USER airflow