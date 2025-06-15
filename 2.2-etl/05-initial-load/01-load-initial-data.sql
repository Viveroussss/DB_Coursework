-- =====================
-- 📥 Initial Data Load
-- =====================

-- Function to load initial data
CREATE OR REPLACE FUNCTION load_initial_data()
RETURNS void AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('load_initial_data', 'STARTED');
    
    BEGIN
        -- Load time dimension for all dates in the data
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
        SELECT DISTINCT
            DATE(play_time),
            TO_CHAR(play_time, 'Day'),
            EXTRACT(DAY FROM play_time),
            TO_CHAR(play_time, 'Month'),
            EXTRACT(MONTH FROM play_time),
            EXTRACT(QUARTER FROM play_time),
            EXTRACT(YEAR FROM play_time),
            EXTRACT(DOW FROM play_time) IN (0, 6),
            FALSE
        FROM player_game
        ON CONFLICT (full_date) DO NOTHING;
        
        -- Load game dimension
        INSERT INTO dim_game (
            game_name,
            game_type,
            min_bet,
            max_bet,
            is_active
        )
        SELECT DISTINCT
            game_name,
            type,
            min_bet,
            max_bet,
            TRUE
        FROM game
        ON CONFLICT (game_name) DO NOTHING;
        
        -- Load location dimension
        INSERT INTO dim_location (
            location_code,
            location_name,
            city,
            state,
            country,
            is_active
        )
        SELECT DISTINCT
            location_code,
            name,
            city,
            state,
            country,
            TRUE
        FROM casino_location
        ON CONFLICT (location_code) DO NOTHING;
        
        -- Load slot machine dimension
        INSERT INTO dim_slot_machine (
            machine_code,
            model,
            is_active
        )
        SELECT DISTINCT
            machine_code,
            model,
            status = 'Online'
        FROM slot_machine
        ON CONFLICT (machine_code) DO NOTHING;
        
        -- Load player dimension
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
        SELECT DISTINCT
            email,
            first_name,
            last_name,
            CASE 
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, dob)) < 25 THEN '18-24'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, dob)) < 35 THEN '25-34'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, dob)) < 45 THEN '35-44'
                ELSE '45+'
            END,
            registration_date,
            CASE 
                WHEN loyalty_points >= 10000 THEN 'Platinum'
                WHEN loyalty_points >= 5000 THEN 'Gold'
                WHEN loyalty_points >= 1000 THEN 'Silver'
                ELSE 'Bronze'
            END,
            CURRENT_TIMESTAMP,
            TRUE,
            1
        FROM player
        ON CONFLICT (player_key) DO NOTHING;
        
        -- Load staff dimension
        INSERT INTO dim_staff (
            staff_key,
            first_name,
            last_name,
            position,
            department,
            hire_date,
            is_active,
            start_date,
            version_number
        )
        SELECT DISTINCT
            staff_email,
            first_name,
            last_name,
            position,
            CASE 
                WHEN position LIKE '%Manager%' THEN 'Management'
                WHEN position LIKE '%Dealer%' THEN 'Gaming'
                WHEN position LIKE '%Security%' THEN 'Security'
                ELSE 'Operations'
            END,
            hire_date,
            TRUE,
            CURRENT_TIMESTAMP,
            1
        FROM staff
        ON CONFLICT (staff_key) DO NOTHING;
        
        -- Load device dimension
        INSERT INTO dim_device (
            device_type,
            device_model,
            is_mobile,
            is_tablet,
            is_desktop
        )
        SELECT DISTINCT
            CASE 
                WHEN device ILIKE '%Mobile%' THEN 'Mobile'
                WHEN device ILIKE '%Tablet%' THEN 'Tablet'
                ELSE 'Desktop'
            END,
            device,
            device ILIKE '%Mobile%',
            device ILIKE '%Tablet%',
            NOT (device ILIKE '%Mobile%' OR device ILIKE '%Tablet%')
        FROM login_history
        ON CONFLICT (device_type, device_model) DO NOTHING;
        
        -- Log success
        PERFORM log_etl_operation('load_initial_data', 'COMPLETED', 1);
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'load_initial_data',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql; 