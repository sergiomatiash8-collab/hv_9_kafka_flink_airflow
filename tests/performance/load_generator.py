import json
import time
import random
from kafka import KafkaProducer

# Configuration
TOPIC = 'tweets'
BROKERS = ['localhost:9092']
TOTAL_RECORDS = 100_000 

def run_load_test():
    # Optimized producer for high load
    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Group messages by 64KB or wait 50ms before sending a batch
        batch_size=65536,
        linger_ms=50,
        compression_type='gzip' # Compression saves bandwidth for large volumes
    )

    print(f"Starting Load Test: {TOTAL_RECORDS} messages...")
    print(f"Topic: {TOPIC} | Brokers: {BROKERS}")
    
    start_time = time.time()

    try:
        for i in range(1, TOTAL_RECORDS + 1):
            # IMPORTANT: Field names must exactly match the CREATE TABLE in Flink
            tweet = {
                "author_id": i,  
                "text": f"Performance test tweet #{i} | Generate load for Flink & Postgres #{random.randint(100, 999)}",
                "created_at": time.strftime('%Y-%m-%d %H:%M:%S'), # Format easily digestible by the DB
                "user_name": f"bot_tester_{random.randint(1, 500)}"
            }
            
            producer.send(TOPIC, tweet)

            if i % 20000 == 0:
                print(f"Progress: {i} sent...")

        print("Finalizing transmission (flushing)...")
        producer.flush()
        
        end_time = time.time()
        duration = end_time - start_time
        
        print("\n" + "="*30)
        print("TEST COMPLETED")
        print(f"Execution time: {duration:.2f} sec")
        print(f"Throughput: {int(TOTAL_RECORDS / duration)} msg/sec")
        print("="*30)

    except Exception as e:
        print(f"Error during generation: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    run_load_test()