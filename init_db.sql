-- 1. Cleanup (Optional: useful for resetting the environment during testing)
-- DROP VIEW IF EXISTS company_stats;
-- DROP TABLE IF EXISTS tweets_enriched;

-- 2. Main Table Creation
CREATE TABLE IF NOT EXISTS tweets_enriched (
    id SERIAL PRIMARY KEY,
    author_id VARCHAR(100) NOT NULL,
    
    -- Using TIMESTAMP WITH TIME ZONE to handle cross-region data sources correctly
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Field 'text' aligns perfectly with the Tweet entity class
    text TEXT NOT NULL,
    
    text_length INT,
    company VARCHAR(50),
    priority VARCHAR(20),
    
    -- Metadata field to track database ingestion time (useful for latency monitoring)
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Idempotency constraint: prevents duplicate entries if the pipeline retries an event
    CONSTRAINT unique_tweet UNIQUE(author_id, created_at)
);

-- 3. Index Creation (Foundation for performance testing and fast lookups)
CREATE INDEX IF NOT EXISTS idx_company ON tweets_enriched(company);
CREATE INDEX IF NOT EXISTS idx_priority ON tweets_enriched(priority);
CREATE INDEX IF NOT EXISTS idx_created_at ON tweets_enriched(created_at);
CREATE INDEX IF NOT EXISTS idx_processed_at ON tweets_enriched(processed_at);

-- 4. Analytical View for Real-time Statistics
CREATE OR REPLACE VIEW company_stats AS
SELECT 
    company,
    COUNT(*) as total_tweets,
    AVG(text_length) as avg_length,
    COUNT(*) FILTER (WHERE priority = 'HIGH') as high_priority_count
FROM tweets_enriched
GROUP BY company;

-- 5. Schema Documentation
COMMENT ON TABLE tweets_enriched IS 'Enriched tweets processed via the Kafka and Flink Pipeline';