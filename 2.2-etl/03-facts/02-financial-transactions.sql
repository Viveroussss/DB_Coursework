-- =====================
-- 💰 Financial Transactions Fact ETL
-- =====================

-- Function to handle financial transactions fact table with improved error handling
CREATE OR REPLACE FUNCTION update_financial_transactions()
RETURNS TRIGGER AS $$
DECLARE
    v_time_id INTEGER;
    v_player_id INTEGER;
    v_location_id INTEGER;
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('update_financial_transactions', 'STARTED');
    
    BEGIN
        -- Get or create time dimension
        v_time_id := get_or_create_time_dim(NEW.transaction_time);
        
        -- Get player dimension ID
        SELECT player_id INTO v_player_id
        FROM dim_player
        WHERE player_key = NEW.player_email
        AND is_current = TRUE;
        
        IF v_player_id IS NULL THEN
            RAISE EXCEPTION 'Player not found in dimension: %', NEW.player_email;
        END IF;
        
        -- Get location dimension ID
        SELECT location_id INTO v_location_id
        FROM dim_location
        WHERE location_code = NEW.location_code;
        
        IF v_location_id IS NULL THEN
            RAISE EXCEPTION 'Location not found: %', NEW.location_code;
        END IF;
        
        -- Insert into fact table
        INSERT INTO fact_financial_transactions (
            time_id,
            player_id,
            transaction_type,
            amount,
            payment_method,
            location_id
        )
        VALUES (
            v_time_id,
            v_player_id,
            NEW.type,
            NEW.amount,
            NEW.payment_method,
            v_location_id
        );
        
        -- Log success
        PERFORM log_etl_operation('update_financial_transactions', 'COMPLETED', 1);
        
        RETURN NEW;
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'update_financial_transactions',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to load initial financial transactions data
CREATE OR REPLACE FUNCTION load_financial_transactions()
RETURNS void AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('load_financial_transactions', 'STARTED');
    
    BEGIN
        -- Process all financial transactions
        INSERT INTO fact_financial_transactions (
            time_id,
            player_id,
            transaction_type,
            amount,
            payment_method,
            location_id
        )
        SELECT 
            get_or_create_time_dim(t.transaction_time),
            dp.player_id,
            t.type,
            t.amount,
            t.payment_method,
            dl.location_id
        FROM transaction t
        JOIN dim_player dp ON dp.player_key = t.player_email AND dp.is_current = TRUE
        LEFT JOIN dim_location dl ON dl.location_code = t.location_code
        WHERE NOT EXISTS (
            SELECT 1 
            FROM fact_financial_transactions fft 
            WHERE fft.time_id = get_or_create_time_dim(t.transaction_time)
            AND fft.player_id = dp.player_id
            AND fft.transaction_type = t.type
            AND fft.amount = t.amount
        );
        
        -- Log success
        PERFORM log_etl_operation('load_financial_transactions', 'COMPLETED', 1);
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'load_financial_transactions',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql; 