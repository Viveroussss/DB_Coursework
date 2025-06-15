-- ============================
-- Function: load_player_game_from_csv (work)
-- ============================

DROP FUNCTION IF EXISTS load_player_game_from_csv(text);





CREATE OR REPLACE FUNCTION load_player_game_from_csv(file_path text)
RETURNS TABLE(inserted_count int, updated_count int) AS
$$
DECLARE
    total_rows int;
    filtered_rows int;
BEGIN
    -- Create temp table with text columns for safe CSV loading
    CREATE TEMP TABLE tmp_player_game (
        player_email TEXT,
        game_name TEXT,
        play_time TEXT,
        amount_bet TEXT,
        amount_won TEXT
    ) ON COMMIT DROP;

    -- Load data with FORCE_NULL to handle empty fields
    EXECUTE format('COPY tmp_player_game FROM %L WITH (FORMAT csv, HEADER true, FORCE_NULL (player_email, game_name, play_time, amount_bet, amount_won))', file_path);

    -- Remove any completely empty rows
    DELETE FROM tmp_player_game
    WHERE player_email IS NULL
      AND game_name IS NULL
      AND play_time IS NULL
      AND amount_bet IS NULL
      AND amount_won IS NULL;

    SELECT COUNT(*) INTO total_rows FROM tmp_player_game;
    RAISE NOTICE 'Total rows loaded (raw): %', total_rows;

    -- Create table with correct types and filter only valid records
    DROP TABLE IF EXISTS dedup;

    CREATE TEMP TABLE dedup AS
    SELECT DISTINCT ON (player_email, game_name, play_time_parsed)
        TRIM(player_email) AS player_email,
        TRIM(game_name) AS game_name,
        play_time_parsed AS play_time,
        amount_bet_num AS amount_bet,
        amount_won_num AS amount_won
    FROM (
        SELECT
            player_email,
            game_name,
            -- Try to convert to timestamp, if fails - NULL
            CASE
                WHEN play_time ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$' THEN play_time::timestamp
                ELSE NULL
            END AS play_time_parsed,
            -- Convert to NUMERIC, NULL if invalid
            CASE WHEN amount_bet ~ '^\d+(\.\d+)?$' THEN amount_bet::numeric(10,2) ELSE NULL END AS amount_bet_num,
            CASE WHEN amount_won ~ '^\d+(\.\d+)?$' THEN amount_won::numeric(10,2) ELSE NULL END AS amount_won_num
        FROM tmp_player_game
        WHERE player_email IS NOT NULL
          AND game_name IS NOT NULL
    ) sub
    WHERE
        player_email IS NOT NULL
        AND player_email IN (SELECT email FROM player)
        AND game_name IS NOT NULL
        AND game_name IN (SELECT game_name FROM game)
        AND play_time_parsed IS NOT NULL
        AND amount_bet_num IS NOT NULL
        AND amount_won_num IS NOT NULL
    ORDER BY player_email, game_name, play_time_parsed DESC;

    SELECT COUNT(*) INTO filtered_rows FROM dedup;
    RAISE NOTICE 'Filtered valid rows to insert: %', filtered_rows;

    -- Upsert into player_game table
    RETURN QUERY
    WITH upsert AS (
        INSERT INTO player_game(player_email, game_name, play_time, amount_bet, amount_won)
        SELECT player_email, game_name, play_time, amount_bet, amount_won FROM dedup
        ON CONFLICT (player_email, game_name, play_time) DO UPDATE SET
            amount_bet = EXCLUDED.amount_bet,
            amount_won = EXCLUDED.amount_won
        WHERE
            player_game.amount_bet IS DISTINCT FROM EXCLUDED.amount_bet OR
            player_game.amount_won IS DISTINCT FROM EXCLUDED.amount_won
        RETURNING xmax = 0 AS inserted
    )
    SELECT
        COUNT(*) FILTER (WHERE inserted)::INT,
        COUNT(*) FILTER (WHERE NOT inserted)::INT
    FROM upsert;
END;
$$ LANGUAGE plpgsql;





SELECT * FROM load_player_game_from_csv('/csvs/player_game.csv');

SELECT * FROM player_game;