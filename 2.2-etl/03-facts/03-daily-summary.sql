-- =====================
-- 📊 Daily Summary Aggregation
-- =====================

-- Function to update daily gaming summary with improved error handling
CREATE OR REPLACE FUNCTION update_daily_gaming_summary()
RETURNS TRIGGER AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('update_daily_gaming_summary', 'STARTED');
    
    BEGIN
        -- Update daily summary for the affected location and game
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
            NEW.time_id,
            NEW.location_id,
            NEW.game_id,
            COUNT(DISTINCT player_id),
            SUM(bet_amount),
            SUM(win_amount),
            SUM(net_result),
            AVG(bet_amount),
            COUNT(*)
        FROM fact_gaming_activity
        WHERE time_id = NEW.time_id
        AND location_id = NEW.location_id
        AND game_id = NEW.game_id
        GROUP BY time_id, location_id, game_id
        ON CONFLICT (time_id, location_id, game_id) DO UPDATE
        SET total_players = EXCLUDED.total_players,
            total_bets = EXCLUDED.total_bets,
            total_wins = EXCLUDED.total_wins,
            net_revenue = EXCLUDED.net_revenue,
            average_bet_amount = EXCLUDED.average_bet_amount,
            total_plays = EXCLUDED.total_plays;
        
        -- Log success
        PERFORM log_etl_operation('update_daily_gaming_summary', 'COMPLETED', 1);
        
        RETURN NEW;
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'update_daily_gaming_summary',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to load initial daily gaming summary data
CREATE OR REPLACE FUNCTION load_daily_gaming_summary()
RETURNS void AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('load_daily_gaming_summary', 'STARTED');
    
    BEGIN
        -- Process all daily summaries
        INSERT INTO agg_daily_gaming_summary (
            time_id,
            player_id,
            game_id,
            total_plays,
            total_bet_amount,
            total_win_amount,
            net_result,
            average_bet,
            win_rate
        )
        SELECT 
            fga.time_id,
            fga.player_id,
            fga.game_id,
            COUNT(*),
            SUM(fga.bet_amount),
            SUM(fga.win_amount),
            SUM(fga.net_result),
            AVG(fga.bet_amount),
            CASE 
                WHEN SUM(fga.bet_amount) > 0 
                THEN SUM(fga.win_amount)::float / SUM(fga.bet_amount)
                ELSE 0 
            END
        FROM fact_gaming_activity fga
        WHERE NOT EXISTS (
            SELECT 1 
            FROM agg_daily_gaming_summary fdgs
            WHERE fdgs.time_id = fga.time_id
            AND fdgs.player_id = fga.player_id
            AND fdgs.game_id = fga.game_id
        )
        GROUP BY fga.time_id, fga.player_id, fga.game_id;
        
        -- Log success
        PERFORM log_etl_operation('load_daily_gaming_summary', 'COMPLETED', 1);
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'load_daily_gaming_summary',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql; 