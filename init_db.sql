-- Створення таблиці для збагачених твітів
CREATE TABLE IF NOT EXISTS tweets_enriched (
    id SERIAL PRIMARY KEY,
    author_id VARCHAR(50) NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    tweet_text TEXT NOT NULL,
    text_length INT,
    company VARCHAR(50),
    priority VARCHAR(20),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Унікальність, щоб уникнути дублікатів при повторних запусках
    UNIQUE(author_id, created_at)
);

-- Індекси для швидкої фільтрації та пошуку
CREATE INDEX idx_company ON tweets_enriched(company);
CREATE INDEX idx_priority ON tweets_enriched(priority);
CREATE INDEX idx_created_at ON tweets_enriched(created_at);
CREATE INDEX idx_processed_at ON tweets_enriched(processed_at);

-- View для швидкої статистики (зручно для Grafana або звітів)
CREATE OR REPLACE VIEW company_stats AS
SELECT 
    company,
    COUNT(*) as total_tweets,
    AVG(text_length) as avg_length,
    SUM(CASE WHEN priority = 'HIGH' THEN 1 ELSE 0 END) as high_priority_count
FROM tweets_enriched
GROUP BY company;

COMMENT ON TABLE tweets_enriched IS 'Збагачені твіти з Flink pipeline';