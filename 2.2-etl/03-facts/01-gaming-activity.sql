-- =====================
-- 🎲 Gaming Activity Fact ETL
-- =====================

-- Function to handle gaming activity fact table with improved error handling
CREATE OR REPLACE FUNCTION update_gaming_activity()
RETURNS TRIGGER AS $$
DECLARE
    v_time_id INTEGER;
    v_player_id INTEGER;
    v_game_id INTEGER;
    v_location_id INTEGER;
    v_slot_machine_id INTEGER;
    v_staff_id INTEGER;
    v_device_id INTEGER;
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('update_gaming_activity', 'STARTED');
    
    BEGIN
        -- Get or create time dimension
        v_time_id := get_or_create_time_dim(NEW.play_time);
        
        -- Get player dimension ID
        SELECT player_id INTO v_player_id
        FROM dim_player
        WHERE player_key = NEW.player_email
        AND is_current = TRUE;
        
        IF v_player_id IS NULL THEN
            RAISE EXCEPTION 'Player not found in dimension: %', NEW.player_email;
        END IF;
        
        -- Get game dimension ID
        SELECT game_id INTO v_game_id
        FROM dim_game
        WHERE game_name = NEW.game_name;
        
        IF v_game_id IS NULL THEN
            RAISE EXCEPTION 'Game not found in dimension: %', NEW.game_name;
        END IF;
        
        -- Get location dimension ID
        IF NEW.table_code IS NOT NULL THEN
            SELECT l.location_id INTO v_location_id
            FROM table_game t
            JOIN dim_location l ON l.location_code = t.location_code
            WHERE t.table_code = NEW.table_code;
            
            -- Get staff ID for table games
            SELECT s.staff_id INTO v_staff_id
            FROM staff_assigned_tables sat
            JOIN dim_staff s ON s.staff_key = sat.staff_email
            WHERE sat.table_code = NEW.table_code
            AND s.is_current = TRUE
            LIMIT 1;
        ELSIF NEW.machine_code IS NOT NULL THEN
            SELECT l.location_id INTO v_location_id
            FROM slot_machine s
            JOIN dim_location l ON l.location_code = s.location_code
            WHERE s.machine_code = NEW.machine_code;
        END IF;
        
        IF v_location_id IS NULL THEN
            RAISE EXCEPTION 'Location not found for table/machine: %', COALESCE(NEW.table_code, NEW.machine_code);
        END IF;
        
        -- Get slot machine dimension ID if applicable
        IF NEW.machine_code IS NOT NULL THEN
            SELECT slot_machine_id INTO v_slot_machine_id
            FROM dim_slot_machine
            WHERE machine_code = NEW.machine_code;
            
            IF v_slot_machine_id IS NULL THEN
                RAISE EXCEPTION 'Slot machine not found in dimension: %', NEW.machine_code;
            END IF;
        END IF;
        
        -- Get device ID from latest login
        SELECT d.device_id INTO v_device_id
        FROM login_history lh
        JOIN dim_device d ON d.device_type = CASE 
                WHEN lh.device ILIKE '%Mobile%' THEN 'Mobile'
                WHEN lh.device ILIKE '%Tablet%' THEN 'Tablet'
                ELSE 'Desktop'
            END
        WHERE lh.player_email = NEW.player_email
        ORDER BY lh.login_time DESC
        LIMIT 1;
        
        -- Insert into fact table
        INSERT INTO fact_gaming_activity (
            time_id,
            player_id,
            game_id,
            location_id,
            slot_machine_id,
            staff_id,
            device_id,
            bet_amount,
            win_amount,
            net_result,
            plays_count
        )
        VALUES (
            v_time_id,
            v_player_id,
            v_game_id,
            v_location_id,
            v_slot_machine_id,
            v_staff_id,
            v_device_id,
            NEW.amount_bet,
            NEW.amount_won,
            COALESCE(NEW.amount_won, 0) - COALESCE(NEW.amount_bet, 0),
            1
        );
        
        -- Update bridge table
        INSERT INTO bridge_game_player (
            player_id,
            game_id,
            first_play_date,
            last_play_date,
            total_plays,
            total_bet_amount,
            total_win_amount
        )
        VALUES (
            v_player_id,
            v_game_id,
            NEW.play_time,
            NEW.play_time,
            1,
            NEW.amount_bet,
            NEW.amount_won
        )
        ON CONFLICT (player_id, game_id) DO UPDATE
        SET last_play_date = NEW.play_time,
            total_plays = bridge_game_player.total_plays + 1,
            total_bet_amount = bridge_game_player.total_bet_amount + NEW.amount_bet,
            total_win_amount = bridge_game_player.total_win_amount + COALESCE(NEW.amount_won, 0);
        
        -- Log success
        PERFORM log_etl_operation('update_gaming_activity', 'COMPLETED', 1);
        
        RETURN NEW;
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'update_gaming_activity',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to load initial gaming activity data
CREATE OR REPLACE FUNCTION process_gaming_activity()
RETURNS void AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('process_gaming_activity', 'STARTED');
    
    BEGIN
        -- Process all gaming activities
        INSERT INTO fact_gaming_activity (
            time_id,
            player_id,
            game_id,
            location_id,
            slot_machine_id,
            staff_id,
            device_id,
            bet_amount,
            win_amount,
            net_result,
            plays_count
        )
        SELECT 
            get_or_create_time_dim(pg.play_time),
            dp.player_id,
            dg.game_id,
            dl.location_id,
            dsm.slot_machine_id,
            ds.staff_id,
            dd.device_id,
            pg.amount_bet,
            pg.amount_won,
            COALESCE(pg.amount_won, 0) - COALESCE(pg.amount_bet, 0),
            1
        FROM player_game pg
        JOIN dim_player dp ON dp.player_key = pg.player_email AND dp.is_current = TRUE
        JOIN dim_game dg ON dg.game_name = pg.game_name
        LEFT JOIN table_game tg ON tg.game_name = pg.game_name
        LEFT JOIN slot_machine sm ON sm.machine_code = pg.machine_code
        LEFT JOIN dim_location dl ON dl.location_code = COALESCE(tg.location_code, sm.location_code)
        LEFT JOIN dim_slot_machine dsm ON dsm.machine_code = pg.machine_code
        LEFT JOIN staff_assigned_tables sat ON sat.table_code = tg.table_code
        LEFT JOIN dim_staff ds ON ds.staff_key = sat.staff_email AND ds.is_current = TRUE
        LEFT JOIN LATERAL (
            SELECT d.device_id
            FROM login_history lh
            JOIN dim_device d ON d.device_type = CASE 
                    WHEN lh.device ILIKE '%Mobile%' THEN 'Mobile'
                    WHEN lh.device ILIKE '%Tablet%' THEN 'Tablet'
                    ELSE 'Desktop'
                END
            WHERE lh.player_email = pg.player_email
            ORDER BY lh.login_time DESC
            LIMIT 1
        ) dd ON true
        WHERE NOT EXISTS (
            SELECT 1 
            FROM fact_gaming_activity fga 
            WHERE fga.time_id = get_or_create_time_dim(pg.play_time)
            AND fga.player_id = dp.player_id
            AND fga.game_id = dg.game_id
        );
        
        -- Log success
        PERFORM log_etl_operation('process_gaming_activity', 'COMPLETED', 1);
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'process_gaming_activity',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql; 