import logging
import sys
from src.application.use_cases.consume_tweets import ConsumeEnrichedTweetsUseCase
from src.infrastructure.config.settings import settings

# Налаштування логування
logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

def main():
    """Точка входу для воркера-консьюмера"""
    logger.info("=== Starting Enriched Tweets Consumer Worker ===")
    
    try:
        use_case = ConsumeEnrichedTweetsUseCase()
        use_case.execute()
    except KeyboardInterrupt:
        logger.info("[!] Consumer stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"[!!!] Unhandled exception in worker: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()