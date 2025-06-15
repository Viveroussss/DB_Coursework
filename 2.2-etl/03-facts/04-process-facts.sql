-- =====================
-- 📊 Process Fact Tables
-- =====================

-- Function to process gaming activity fact table
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
        LEFT JOIN slot_play sp ON sp.player_email = pg.player_email AND sp.play_time = pg.play_time
        LEFT JOIN slot_machine sm ON sm.machine_code = sp.machine_code
        LEFT JOIN dim_location dl ON dl.location_code = COALESCE(tg.location_code, sm.location_code)
        LEFT JOIN dim_slot_machine dsm ON dsm.machine_code = sm.machine_code
        LEFT JOIN staff_assigned_tables sat ON sat.table_code = tg.table_code
        LEFT JOIN dim_staff ds ON ds.staff_key = sat.staff_email AND ds.is_active = TRUE
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
        SELECT 
            dp.player_id,
            dg.game_id,
            MIN(pg.play_time),
            MAX(pg.play_time),
            COUNT(*),
            SUM(pg.amount_bet),
            SUM(COALESCE(pg.amount_won, 0))
        FROM player_game pg
        JOIN dim_player dp ON dp.player_key = pg.player_email AND dp.is_current = TRUE
        JOIN dim_game dg ON dg.game_name = pg.game_name
        GROUP BY dp.player_id, dg.game_id
        ON CONFLICT (player_id, game_id) DO UPDATE
        SET last_play_date = EXCLUDED.last_play_date,
            total_plays = EXCLUDED.total_plays,
            total_bet_amount = EXCLUDED.total_bet_amount,
            total_win_amount = EXCLUDED.total_win_amount;
        
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

-- Function to process financial transactions fact table
CREATE OR REPLACE FUNCTION process_financial_transactions()
RETURNS void AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('process_financial_transactions', 'STARTED');
    
    BEGIN
        -- Process new financial transactions
        INSERT INTO fact_financial_transactions (
            time_id,
            player_id,
            location_id,
            staff_id,
            transaction_type,
            amount,
            loyalty_points_earned,
            loyalty_points_redeemed
        )
        SELECT 
            get_or_create_time_dim(t.transaction_time),
            dp.player_id,
            dl.location_id,
            ds.staff_id,
            t.transaction_type,
            t.amount,
            CASE 
                WHEN t.transaction_type = 'Bet' THEN FLOOR(t.amount)
                ELSE 0
            END,
            CASE 
                WHEN t.transaction_type = 'Withdraw' THEN FLOOR(t.amount)
                ELSE 0
            END
        FROM transaction t
        JOIN dim_player dp ON dp.player_key = t.player_email AND dp.is_current = TRUE
        LEFT JOIN dim_game dg ON dg.game_name = t.game_name
        LEFT JOIN table_game tg ON tg.game_name = t.game_name
        LEFT JOIN dim_location dl ON dl.location_code = tg.location_code
        LEFT JOIN staff_assigned_tables sat ON sat.table_code = tg.table_code
        LEFT JOIN dim_staff ds ON ds.staff_key = sat.staff_email AND ds.is_active = TRUE
        WHERE NOT EXISTS (
            SELECT 1 
            FROM fact_financial_transactions fft 
            WHERE fft.time_id = get_or_create_time_dim(t.transaction_time)
            AND fft.player_id = dp.player_id
            AND fft.transaction_type = t.transaction_type
            AND fft.amount = t.amount
        );
        
        -- Log success
        PERFORM log_etl_operation('process_financial_transactions', 'COMPLETED', 1);
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'process_financial_transactions',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to process daily gaming summary
CREATE OR REPLACE FUNCTION process_daily_gaming_summary()
RETURNS void AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('process_daily_gaming_summary', 'STARTED');
    
    BEGIN
        -- Process daily gaming summary
        INSERT INTO agg_daily_gaming_summary (
            time_id,
            location_id,
            game_id,
            total_players,
            total_bets,
            total_wins,
            net_revenue,
            average_bet_amount,
            total_plays
        )
        SELECT 
            dt.time_id,
            fga.location_id,
            fga.game_id,
            COUNT(DISTINCT fga.player_id) as total_players,
            SUM(fga.bet_amount) as total_bets,
            SUM(fga.win_amount) as total_wins,
            SUM(fga.net_result) as net_revenue,
            AVG(fga.bet_amount) as average_bet_amount,
            COUNT(*) as total_plays
        FROM fact_gaming_activity fga
        JOIN dim_time dt ON dt.time_id = fga.time_id
        GROUP BY dt.time_id, fga.location_id, fga.game_id
        ON CONFLICT (time_id, location_id, game_id) DO UPDATE
        SET total_players = EXCLUDED.total_players,
            total_bets = EXCLUDED.total_bets,
            total_wins = EXCLUDED.total_wins,
            net_revenue = EXCLUDED.net_revenue,
            average_bet_amount = EXCLUDED.average_bet_amount,
            total_plays = EXCLUDED.total_plays;
        
        -- Log success
        PERFORM log_etl_operation('process_daily_gaming_summary', 'COMPLETED', 1);
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'process_daily_gaming_summary',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql; 