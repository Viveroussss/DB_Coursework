-- =====================
-- 👥 Player Dimension ETL
-- =====================

-- Function to handle player dimension (Type 2 SCD) with improved error handling
CREATE OR REPLACE FUNCTION update_player_dimension()
RETURNS TRIGGER AS $$
DECLARE
    v_player_id INTEGER;
    v_current_record RECORD;
    v_log_id INTEGER;
    v_age_group VARCHAR(20);
    v_loyalty_tier VARCHAR(20);
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('update_player_dimension', 'STARTED');
    
    BEGIN
        -- Calculate derived attributes
        v_age_group := CASE 
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, NEW.dob)) < 25 THEN '18-24'
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, NEW.dob)) < 35 THEN '25-34'
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, NEW.dob)) < 45 THEN '35-44'
            ELSE '45+'
        END;
        
        v_loyalty_tier := CASE 
            WHEN NEW.loyalty_points >= 10000 THEN 'Platinum'
            WHEN NEW.loyalty_points >= 5000 THEN 'Gold'
            WHEN NEW.loyalty_points >= 1000 THEN 'Silver'
            ELSE 'Bronze'
        END;
        
        -- Check if player exists in dimension
        SELECT player_id, is_current INTO v_current_record
        FROM dim_player
        WHERE player_key = NEW.email
        AND is_current = TRUE;
        
        -- If player doesn't exist, create new record
        IF v_current_record.player_id IS NULL THEN
            INSERT INTO dim_player (
                player_key,
                first_name,
                last_name,
                age_group,
                registration_date,
                loyalty_tier,
                start_date,
                is_current,
                version_number
            )
            VALUES (
                NEW.email,
                NEW.first_name,
                NEW.last_name,
                v_age_group,
                NEW.registration_date,
                v_loyalty_tier,
                CURRENT_TIMESTAMP,
                TRUE,
                1
            )
            RETURNING player_id INTO v_player_id;
        ELSE
            -- Check if any relevant attributes changed
            IF EXISTS (
                SELECT 1
                FROM dim_player
                WHERE player_id = v_current_record.player_id
                AND (
                    first_name != NEW.first_name
                    OR last_name != NEW.last_name
                    OR loyalty_tier != v_loyalty_tier
                )
            ) THEN
                -- Close current record
                UPDATE dim_player
                SET is_current = FALSE,
                    end_date = CURRENT_TIMESTAMP
                WHERE player_id = v_current_record.player_id;
                
                -- Create new record
                INSERT INTO dim_player (
                    player_key,
                    first_name,
                    last_name,
                    age_group,
                    registration_date,
                    loyalty_tier,
                    start_date,
                    is_current,
                    version_number
                )
                VALUES (
                    NEW.email,
                    NEW.first_name,
                    NEW.last_name,
                    v_age_group,
                    NEW.registration_date,
                    v_loyalty_tier,
                    CURRENT_TIMESTAMP,
                    TRUE,
                    (SELECT version_number + 1 FROM dim_player WHERE player_id = v_current_record.player_id)
                )
                RETURNING player_id INTO v_player_id;
            ELSE
                v_player_id := v_current_record.player_id;
            END IF;
        END IF;
        
        -- Log success
        PERFORM log_etl_operation('update_player_dimension', 'COMPLETED', 1);
        
        RETURN NEW;
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'update_player_dimension',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql; 