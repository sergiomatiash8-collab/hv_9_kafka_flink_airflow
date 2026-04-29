import logging
from pyflink.table import EnvironmentSettings, TableEnvironment

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_tweet_enrichment_processor():
    """
    Головна функція Flink Job для збагачення твітів.
    
    Процес:
    1. Читає RAW дані з топіку 'tweets'
    2. Виконує бізнес-логіку збагачення:
       - Додає processed_at (час обробки)
       - Розраховує text_length
       - Визначає company (на основі author_id % 4)
       - Визначає priority (на основі text_length)
    3. Записує ЗБАГАЧЕНІ дані в топік 'tweets_enriched'
    """
    
    logger.info("=" * 70)
    logger.info("🚀 FLINK TWEET ENRICHMENT PROCESSOR - STARTING")
    logger.info("=" * 70)
    
    # ==================== 1. СТВОРЕННЯ СТРІМІНГОВОГО ОТОЧЕННЯ ====================
    logger.info("📋 Крок 1: Створення Flink StreamTableEnvironment...")
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)
    
    # Налаштування обробки помилок
    t_env.get_config().set("table.exec.sink.not-null-enforcer", "DROP")
    t_env.get_config().set("pipeline.name", "TweetEnrichmentPipeline")
    
    logger.info("✅ Flink Environment готове")
    
    # ==================== 2. СТВОРЕННЯ SOURCE TABLE (KAFKA INPUT) ====================
    logger.info("📥 Крок 2: Створення Source Table для топіку 'tweets'...")
    
    t_env.execute_sql("""
        CREATE TABLE tweets_source (
            author_id STRING,
            created_at STRING,
            text STRING,
            -- Для event-time processing (опціонально)
            event_time AS TO_TIMESTAMP(created_at),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'tweets',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-enrichment-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.fail-on-missing-field' = 'false'
        )
    """)
    
    logger.info("✅ Source Table створена (topic: tweets)")
    
    # ==================== 3. СТВОРЕННЯ SINK TABLE (KAFKA OUTPUT) ====================
    logger.info("📤 Крок 3: Створення Sink Table для топіку 'tweets_enriched'...")
    
    t_env.execute_sql("""
        CREATE TABLE tweets_enriched_sink (
            author_id STRING,
            created_at STRING,
            text STRING,
            text_length INT,
            processed_at STRING,
            company STRING,
            priority STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'tweets_enriched',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false'
        )
    """)
    
    logger.info("✅ Sink Table створена (topic: tweets_enriched)")
    
    # ==================== 4. ЛОГІКА ЗБАГАЧЕННЯ ====================
    logger.info("🔄 Крок 4: Запуск збагачення та запису даних...")
    
    enrichment_query = """
        INSERT INTO tweets_enriched_sink
        SELECT 
            -- ========== ОРИГІНАЛЬНІ ПОЛЯ ==========
            author_id,
            created_at,
            text,
            
            -- ========== ЗБАГАЧЕНІ ПОЛЯ ==========
            
            -- 1. text_length: Довжина тексту (корисна метрика для аналітики)
            CHAR_LENGTH(text) as text_length,
            
            -- 2. processed_at: Timestamp обробки (для моніторингу latency)
            CAST(CURRENT_TIMESTAMP AS STRING) as processed_at,
            
            -- 3. company: Визначення компанії на основі author_id
            --    Логіка: author_id % 4 → кожен 4-й автор належить іншій компанії
            CASE 
                WHEN MOD(CAST(author_id AS BIGINT), 4) = 0 THEN 'Apple'
                WHEN MOD(CAST(author_id AS BIGINT), 4) = 1 THEN 'Google'
                WHEN MOD(CAST(author_id AS BIGINT), 4) = 2 THEN 'Microsoft'
                ELSE 'Amazon'
            END as company,
            
            -- 4. priority: Визначення пріоритету на основі довжини тексту
            --    Логіка: довгі твіти = HIGH, середні = MEDIUM, короткі = LOW
            CASE 
                WHEN CHAR_LENGTH(text) > 200 THEN 'HIGH'
                WHEN CHAR_LENGTH(text) > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END as priority
            
        FROM tweets_source
        
        -- ========== ФІЛЬТРАЦІЯ НЕВАЛІДНИХ ДАНИХ ==========
        WHERE 
            author_id IS NOT NULL 
            AND text IS NOT NULL
            AND text <> ''
            AND CHAR_LENGTH(text) > 0
    """
    
    # Запуск Query (асинхронно)
    table_result = t_env.execute_sql(enrichment_query)
    
    logger.info("=" * 70)
    logger.info("✅ FLINK JOB УСПІШНО ЗАПУЩЕНА!")
    logger.info("=" * 70)
    logger.info("📊 Статистика:")
    logger.info("   • Source Topic: tweets")
    logger.info("   • Sink Topic: tweets_enriched")
    logger.info("   • Group ID: flink-enrichment-group")
    logger.info("   • Збагачення: text_length, processed_at, company, priority")
    logger.info("=" * 70)
    logger.info("💡 Переглянути Job можна в Flink Dashboard: http://localhost:8081")
    logger.info("=" * 70)
    
    # Job працює безкінечно (streaming mode)
    # Блокуємо головний потік
    table_result.wait()

if __name__ == '__main__':
    try:
        run_tweet_enrichment_processor()
    except KeyboardInterrupt:
        logger.info("⚠️  Отримано сигнал зупинки (CTRL+C)")
    except Exception as e:
        logger.error(f"💥 Критична помилка в Flink Job: {e}", exc_info=True)
        raise