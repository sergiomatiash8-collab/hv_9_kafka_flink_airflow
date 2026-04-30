import logging
import sys

from ...application.use_cases.stream_tweets import StreamTweetsUseCase
from ...domain.services.enrichment_service import EnrichmentService
from ...infrastructure.kafka.producer import KafkaProducerAdapter
from ...infrastructure.file_system.csv_reader import CSVTweetReader
from ...infrastructure.config.settings import settings

# Logging configuration based on settings
logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    Main entry point.
    Here we wire interfaces with real infrastructure implementations.
    """
    logger.info("Initializing application...")

    try:
        # 1. Initialize infrastructure adapters
        tweet_reader = CSVTweetReader(settings.csv_file_path)

        tweet_producer = KafkaProducerAdapter(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            topic=settings.kafka.topic,
        )

        # 2. Initialize domain services
        enrichment_service = EnrichmentService()

        # 3. Compose use case (dependency injection)
        use_case = StreamTweetsUseCase(
            tweet_reader=tweet_reader,
            tweet_producer=tweet_producer,
            enrichment_service=enrichment_service
        )

        # 4. Execute business scenario
        logger.info("Starting streaming business workflow...")
        result = use_case.execute(enrich=True)

        # 5. Handle result
        if result['status'] == 'success':
            logger.info(f"Successfully processed {result['processed_count']} tweets")
            sys.exit(0)
        else:
            logger.error(f"Execution error: {result.get('message')}")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)

    except Exception as e:
        logger.exception(f"Critical startup error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()