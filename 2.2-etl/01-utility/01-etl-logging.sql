-- =====================
-- 🎲 ETL Logging
-- =====================

-- Create ETL logging table
CREATE TABLE IF NOT EXISTS etl_log (
    log_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Function to log ETL operations
CREATE OR REPLACE FUNCTION log_etl_operation(
    p_process_name VARCHAR,
    p_status VARCHAR,
    p_records_processed INTEGER DEFAULT 0,
    p_error_message TEXT DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    INSERT INTO etl_log (
        process_name,
        start_time,
        end_time,
        status,
        records_processed,
        error_message
    )
    VALUES (
        p_process_name,
        CURRENT_TIMESTAMP,
        CASE WHEN p_status = 'COMPLETED' THEN CURRENT_TIMESTAMP ELSE NULL END,
        p_status,
        p_records_processed,
        p_error_message
    )
    RETURNING log_id INTO v_log_id;
    
    RETURN v_log_id;
END;
$$ LANGUAGE plpgsql; 