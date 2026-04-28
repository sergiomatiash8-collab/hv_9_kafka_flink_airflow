-- 1. Очищення (опціонально, корисно для тестів, якщо хочеш починати з чистого листа)
-- DROP VIEW IF EXISTS company_stats;
-- DROP TABLE IF EXISTS tweets_enriched;

-- 2. Створення основної таблиці
CREATE TABLE IF NOT EXISTS tweets_enriched (
    id SERIAL PRIMARY KEY,
    author_id VARCHAR(100) NOT NULL,
    
    -- Використовуємо TIMESTAMP WITH TIME ZONE для коректної роботи з датами з різних джерел
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Поле 'text' замість 'tweet_text' для повної відповідності класу Tweet
    text TEXT NOT NULL,
    
    text_length INT,
    company VARCHAR(50),
    priority VARCHAR(20),
    
    -- Час, коли запис потрапив у базу (корисно для моніторингу затримок/latency)
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Констрейнт для запобігання дублікатів (Idempotency)
    -- Якщо один і той самий твіт прийде двічі, база не створить дублікат
    CONSTRAINT unique_tweet UNIQUE(author_id, created_at)
);

-- 3. Створення індексів (фундамент для Performance тестування)
CREATE INDEX IF NOT EXISTS idx_company ON tweets_enriched(company);
CREATE INDEX IF NOT EXISTS idx_priority ON tweets_enriched(priority);
CREATE INDEX IF NOT EXISTS idx_created_at ON tweets_enriched(created_at);
CREATE INDEX IF NOT EXISTS idx_processed_at ON tweets_enriched(processed_at);

-- 4. Оновлена View для статистики (тепер працює з полем 'text')
CREATE OR REPLACE VIEW company_stats AS
SELECT 
    company,
    COUNT(*) as total_tweets,
    AVG(text_length) as avg_length,
    COUNT(*) FILTER (WHERE priority = 'HIGH') as high_priority_count
FROM tweets_enriched
GROUP BY company;

-- 5. Додаємо коментар для документації схеми
COMMENT ON TABLE tweets_enriched IS 'Збагачені твіти, оброблені через Kafka та Flink Pipeline';