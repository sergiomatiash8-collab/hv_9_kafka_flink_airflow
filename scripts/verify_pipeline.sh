#!/bin/bash

# Скрипт для верифікації пайплайну

echo "========================================="
echo "🔍 ВЕРИФІКАЦІЯ PIPELINE"
echo "========================================="

# 1. Перевірка контейнерів
echo ""
echo "1️⃣ Перевірка запущених контейнерів..."
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "kafka|flink|airflow|producer|consumer"

# 2. Перевірка топіків Kafka
echo ""
echo "2️⃣ Перевірка Kafka топіків..."
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# 3. Перевірка Flink Jobs
echo ""
echo "3️⃣ Перевірка Flink Jobs..."
curl -s http://localhost:8081/jobs | python -m json.tool

# 4. Перевірка CSV файлів
echo ""
echo "4️⃣ Перевірка згенерованих CSV файлів..."
ls -lh output/tweets_*.csv | tail -5

# 5. Приклад вмісту CSV
echo ""
echo "5️⃣ Приклад вмісту останнього CSV файлу..."
LAST_FILE=$(ls -t output/tweets_*.csv | head -1)
if [ -f "$LAST_FILE" ]; then
    echo "Файл: $LAST_FILE"
    head -3 "$LAST_FILE"
fi

echo ""
echo "========================================="
echo "✅ Верифікацію завершено"
echo "========================================="