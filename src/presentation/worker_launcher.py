"""
Worker Launcher - Entry point for the Consumer container.

Starts the Use Case for consuming enriched tweets.
"""

import logging
import sys
from src.application.use_cases.consume_tweets import ConsumeEnrichedTweetsUseCase

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to launch the Worker."""
    logger.info("=" * 70)
    logger.info("WORKER CONSUMER LAUNCHER - STARTING")
    logger.info("=" * 70)
    
    try:
        # Initialize and execute Use Case
        use_case = ConsumeEnrichedTweetsUseCase()
        use_case.execute()
        
    except KeyboardInterrupt:
        logger.info("Stop signal received")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()