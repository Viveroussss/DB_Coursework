-- ============================
-- Function: load_transaction_from_csv (work)
-- ============================
CREATE OR REPLACE FUNCTION load_transaction_from_csv(csv_path TEXT)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    invalid_games TEXT;
    invalid_players TEXT;
BEGIN
    CREATE TEMP TABLE tmp_transaction (
        transaction_code TEXT,
        player_email TEXT,
        amount TEXT,
        transaction_type TEXT,
        transaction_time TEXT,
        game_name TEXT
    );

    EXECUTE format('COPY tmp_transaction FROM %L CSV HEADER', csv_path);

    IF EXISTS (
        SELECT 1 FROM tmp_transaction 
        WHERE transaction_code IS NULL OR player_email IS NULL 
           OR amount IS NULL OR transaction_type IS NULL 
           OR transaction_time !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
           OR amount !~ '^\d+(\.\d{1,2})?$'
    ) THEN
        RAISE EXCEPTION 'Validation failed in tmp_transaction: required fields missing or invalid format.';
    END IF;

    -- Check for invalid games and collect them for warning
    SELECT string_agg(DISTINCT t.game_name, ', ')
    INTO invalid_games
    FROM tmp_transaction t
    LEFT JOIN game g ON t.game_name = g.game_name
    WHERE g.game_name IS NULL;

    -- Check for invalid players and collect them for warning
    SELECT string_agg(DISTINCT t.player_email, ', ')
    INTO invalid_players
    FROM tmp_transaction t
    LEFT JOIN player p ON t.player_email = p.email
    WHERE p.email IS NULL;

    -- Remove records with invalid games or players
    DELETE FROM tmp_transaction t
    WHERE NOT EXISTS (
        SELECT 1 FROM game g WHERE g.game_name = t.game_name
    ) OR NOT EXISTS (
        SELECT 1 FROM player p WHERE p.email = t.player_email
    );

    -- Raise notices about skipped records if any
    IF invalid_games IS NOT NULL THEN
        RAISE NOTICE 'Skipped transactions with invalid game names: %', invalid_games;
    END IF;
    IF invalid_players IS NOT NULL THEN
        RAISE NOTICE 'Skipped transactions with invalid player emails: %', invalid_players;
    END IF;

    INSERT INTO transaction AS t(transaction_code, player_email, amount, transaction_type, transaction_time, game_name)
    SELECT 
        transaction_code,
        player_email,
        amount::NUMERIC(10,2),
        transaction_type,
        transaction_time::TIMESTAMP,
        game_name
    FROM tmp_transaction
    ON CONFLICT (transaction_code) DO UPDATE
    SET player_email = EXCLUDED.player_email,
        amount = EXCLUDED.amount,
        transaction_type = EXCLUDED.transaction_type,
        transaction_time = EXCLUDED.transaction_time,
        game_name = EXCLUDED.game_name
    WHERE t.player_email IS DISTINCT FROM EXCLUDED.player_email
       OR t.amount IS DISTINCT FROM EXCLUDED.amount
       OR t.transaction_type IS DISTINCT FROM EXCLUDED.transaction_type
       OR t.transaction_time IS DISTINCT FROM EXCLUDED.transaction_time
       OR t.game_name IS DISTINCT FROM EXCLUDED.game_name;

    DROP TABLE tmp_transaction;
END;
$$;

SELECT * FROM load_transaction_from_csv('/csvs/transaction.csv');

SELECT * FROM transaction;