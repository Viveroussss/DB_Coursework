-- =====================
-- 🔄 Time Dimension Utility
-- =====================

-- Function to get or create time dimension record with error handling
CREATE OR REPLACE FUNCTION get_or_create_time_dim(p_date TIMESTAMP)
RETURNS INTEGER AS $$
DECLARE
    v_time_id INTEGER;
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('get_or_create_time_dim', 'STARTED');
    
    BEGIN
        -- Check if time record exists
        SELECT time_id INTO v_time_id
        FROM dim_time
        WHERE full_date = DATE(p_date);
        
        -- If not exists, create new record
        IF v_time_id IS NULL THEN
            INSERT INTO dim_time (
                full_date,
                day_of_week,
                day_of_month,
                month_name,
                month_number,
                quarter,
                year,
                is_weekend,
                is_holiday
            )
            VALUES (
                DATE(p_date),
                TO_CHAR(p_date, 'Day'),
                EXTRACT(DAY FROM p_date),
                TO_CHAR(p_date, 'Month'),
                EXTRACT(MONTH FROM p_date),
                EXTRACT(QUARTER FROM p_date),
                EXTRACT(YEAR FROM p_date),
                EXTRACT(DOW FROM p_date) IN (0, 6),
                FALSE
            )
            RETURNING time_id INTO v_time_id;
        END IF;
        
        -- Log success
        PERFORM log_etl_operation('get_or_create_time_dim', 'COMPLETED', 1);
        
        RETURN v_time_id;
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'get_or_create_time_dim',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql; 