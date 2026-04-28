import time
from src.application.use_cases.stream_tweets_use_case import StreamTweetsUseCase
from src.infrastructure.kafka.producer import KafkaProducerAdapter
from src.infrastructure.file_system.csv_reader import CSVTweetReader
from src.infrastructure.config.settings import settings

if __name__ == "__main__":
    # Чекаємо, поки Kafka прокинеться
    time.sleep(10)
    
    reader = CSVTweetReader(settings.csv_file_path)
    producer = KafkaProducerAdapter()
    
    use_case = StreamTweetsUseCase(reader, producer)
    use_case.execute()