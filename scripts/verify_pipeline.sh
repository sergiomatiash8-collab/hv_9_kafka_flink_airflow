#!/bin/bash

# Script for pipeline verification

echo "========================================="
echo "🔍 PIPELINE VERIFICATION"
echo "========================================="

# 1. Container Status Check
echo ""
echo "1️⃣ Checking running containers..."
# Filters for core infrastructure components
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "kafka|flink|airflow|producer|consumer"

# 2. Kafka Topic Verification
echo ""
echo "2️⃣ Checking Kafka topics..."
# Verifies that the 'tweets' topic (and others) actually exists in the broker
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# 3. Flink Job Status
echo ""
echo "3️⃣ Checking Flink Jobs..."
# Queries the Flink REST API to list active/completed streaming jobs
curl -s http://localhost:8081/jobs | python3 -m json.tool

# 4. CSV Output Check
echo ""
echo "4️⃣ Checking generated CSV files..."
# Checks the local output directory for files written by the consumer/Flink
ls -lh output/tweets_*.csv 2>/dev/null | tail -5

# 5. CSV Data Preview
echo ""
echo "5️⃣ Previewing content of the latest CSV file..."
# Picks the most recent file to ensure data is flowing and formatted correctly
LAST_FILE=$(ls -t output/tweets_*.csv 2>/dev/null | head -1)
if [ -f "$LAST_FILE" ]; then
    echo "File: $LAST_FILE"
    head -3 "$LAST_FILE"
else
    echo "[-] No output CSV files found in the output/ directory."
fi

echo ""
echo "========================================="
echo "✅ Verification Complete"
echo "========================================="