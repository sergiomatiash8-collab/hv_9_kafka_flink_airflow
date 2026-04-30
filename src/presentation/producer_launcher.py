"""
Kafka Producer - Entry point for the Producer container.

Responsibilities:
    1. Reading CSV file with RAW data
    2. Sending to 'tweets' topic (without enrichment)
    3. Simulating real data flow with controlled speed

Clean Architecture Layer: Presentation Layer
"""

import pandas as pd
import json
import time
import os
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================

# Environment variables with fallback values
KAFKA_SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'tweets')
CSV_FILE = os.environ.get('CSV_FILE', '/app/raw_tweets.csv')
MESSAGES_PER_SECOND = int(os.environ.get('MESSAGES_PER_SECOND', '2'))

# ==================== PRODUCER CREATION ====================

def create_producer(max_retries: int = 10) -> KafkaProducer:
    """
    Creates Kafka Producer with retry logic.
    
    Args:
        max_retries: Maximum number of connection attempts
        
    Returns:
        Configured KafkaProducer
        
    Raises:
        Exception: if connection fails after all attempts
    """
    retry_count = 0
    retry_delay = 5  # seconds between attempts
    
    while retry_count < max_retries:
        try:
            logger.info(f"Attempt {retry_count + 1}/{max_retries}: connecting to {KAFKA_SERVER}...")
            
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                
                # Reliability settings
                acks='all',  # Wait for all replicas to acknowledge
                retries=5,   # Retries for delivery errors
                
                # Ordering settings
                max_in_flight_requests_per_connection=1,
                
                # Timeout settings
                request_timeout_ms=30000,
                
                # Compression for bandwidth efficiency
                compression_type='gzip',
            )
            
            logger.info(f"Connected to Kafka: {KAFKA_SERVER}")
            return producer
            
        except NoBrokersAvailable as e:
            retry_count += 1
            logger.warning(f"Kafka unavailable: {e}")
            
            if retry_count < max_retries:
                logger.info(f"Waiting {retry_delay} seconds before next attempt...")
                time.sleep(retry_delay)
            else:
                logger.error(f"All connection attempts exhausted ({max_retries})")
                raise
        
        except Exception as e:
            retry_count += 1
            logger.error(f"Error connecting to Kafka: {e}")
            
            if retry_count >= max_retries:
                raise
            
            time.sleep(retry_delay)
    
    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")

# ==================== DATA STREAMING ====================

def stream_data():
    """
    Main function for streaming data from CSV to Kafka.
    
    Process:
        1. Read CSV file
        2. Create Kafka Producer
        3. Send data with controlled speed
        4. Log progress
    """
    # 1. Check if CSV exists
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")
    
    # 2. Read CSV
    logger.info(f"Reading data from: {CSV_FILE}")
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8')
        total_records = len(df)
        logger.info(f"Found {total_records} records")
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        raise
    
    # 3. Create Producer
    producer = create_producer()
    
    # 4. Calculate delay between messages
    delay = 1.0 / MESSAGES_PER_SECOND
    
    # 5. Send data
    sent_count = 0
    error_count = 0
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("DATA TRANSMISSION STARTED")
    logger.info(f"   Topic: {KAFKA_TOPIC}")
    logger.info(f"   Speed: {MESSAGES_PER_SECOND} messages/sec")
    logger.info(f"   Total records: {total_records}")
    logger.info("=" * 70)
    
    try:
        for index, row in df.iterrows():
            # Create message (RAW data, WITHOUT enrichment)
            message = {
                "author_id": str(row['author_id']),
                "created_at": str(row['created_at']),
                "text": str(row['text'])
            }
            
            try:
                # Send to Kafka (asynchronous)
                future = producer.send(KAFKA_TOPIC, value=message)
                
                # Wait for confirmation
                record_metadata = future.get(timeout=10)
                
                sent_count += 1
                
                # Log progress every 50 messages
                if sent_count % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = sent_count / elapsed
                    
                    logger.info(
                        f"Sent: {sent_count}/{total_records} "
                        f"({(sent_count/total_records*100):.1f}%) | "
                        f"Rate: {rate:.2f} msg/sec | "
                        f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}"
                    )
                
            except KafkaError as e:
                error_count += 1
                logger.error(f"Error sending message {index}: {e}")
                continue
            
            # Delay to simulate real-time stream
            time.sleep(delay)
        
        # Flush buffer
        logger.info("Flushing Producer buffer...")
        producer.flush()
        
        # Final statistics
        elapsed = time.time() - start_time
        actual_rate = sent_count / elapsed
        
        logger.info("=" * 70)
        logger.info("TRANSMISSION COMPLETED")
        logger.info("=" * 70)
        logger.info(f"Successfully sent: {sent_count}/{total_records}")
        logger.info(f"Errors: {error_count}")
        logger.info(f"Runtime: {elapsed:.2f} seconds")
        logger.info(f"Actual rate: {actual_rate:.2f} msg/sec")
        logger.info("=" * 70)
        
    except KeyboardInterrupt:
        logger.info("Stopping Producer (SIGINT received)")
        logger.info(f"Sent: {sent_count}/{total_records} before stopping")
        
    finally:
        # Close Producer
        producer.close()
        logger.info("Producer stopped")

# ==================== ENTRY POINT ====================

if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("KAFKA PRODUCER LAUNCHER - STARTING")
    logger.info("=" * 70)
    logger.info(f"Kafka Server: {KAFKA_SERVER}")
    logger.info(f"Target Topic: {KAFKA_TOPIC}")
    logger.info(f"Data Source: {CSV_FILE}")
    logger.info(f"Speed: {MESSAGES_PER_SECOND} messages/second")
    logger.info("=" * 70)
    
    try:
        stream_data()
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        exit(1)