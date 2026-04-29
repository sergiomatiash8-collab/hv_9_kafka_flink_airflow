"""
Worker Launcher - точка входу для Consumer контейнера.

Запускає Use Case для споживання збагачених твітів.
"""

import logging
import sys
from src.application.use_cases.consume_tweets import ConsumeEnrichedTweetsUseCase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Головна функція запуску Worker."""
    logger.info("=" * 70)
    logger.info("🎬 WORKER CONSUMER LAUNCHER - STARTING")
    logger.info("=" * 70)
    
    try:
        # Ініціалізація та запуск Use Case
        use_case = ConsumeEnrichedTweetsUseCase()
        use_case.execute()
        
    except KeyboardInterrupt:
        logger.info("⚠️  Отримано сигнал зупинки")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Критична помилка: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()