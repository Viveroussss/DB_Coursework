-- ============================
-- Function: load_game_result_from_csv (work)
-- ============================
CREATE OR REPLACE FUNCTION load_game_result_from_csv(csv_path TEXT)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    invalid_games TEXT;
    invalid_tables TEXT;
    invalid_records RECORD;
BEGIN
    CREATE TEMP TABLE tmp_game_result (
        result_code TEXT,
        game_name TEXT,
        table_code TEXT,
        result_time TEXT,
        outcome_description TEXT
    );

    EXECUTE format('COPY tmp_game_result FROM %L CSV HEADER', csv_path);

    -- Debug: Show records with missing required fields
    FOR invalid_records IN 
        SELECT * FROM tmp_game_result 
        WHERE result_code IS NULL OR game_name IS NULL 
           OR result_time IS NULL OR outcome_description IS NULL
    LOOP
        RAISE NOTICE 'Record with missing required fields: %', invalid_records;
    END LOOP;

    -- Debug: Show records with invalid date format
    FOR invalid_records IN 
        SELECT * FROM tmp_game_result 
        WHERE result_time !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
    LOOP
        RAISE NOTICE 'Record with invalid date format: %', invalid_records;
    END LOOP;

    -- Remove invalid records instead of raising exception
    DELETE FROM tmp_game_result 
    WHERE result_code IS NULL 
       OR game_name IS NULL 
       OR result_time IS NULL 
       OR outcome_description IS NULL
       OR result_time !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
       OR table_code IS NULL OR table_code = '';

    -- Check for invalid games and collect them for warning
    SELECT string_agg(DISTINCT t.game_name, ', ')
    INTO invalid_games
    FROM tmp_game_result t
    LEFT JOIN game g ON t.game_name = g.game_name
    WHERE g.game_name IS NULL;

    -- Check for invalid tables and collect them for warning
    SELECT string_agg(DISTINCT t.table_code, ', ')
    INTO invalid_tables
    FROM tmp_game_result t
    LEFT JOIN table_game tg ON t.table_code = tg.table_code
    WHERE tg.table_code IS NULL;

    -- Remove records with invalid games or tables
    DELETE FROM tmp_game_result t
    WHERE NOT EXISTS (
        SELECT 1 FROM game g WHERE g.game_name = t.game_name
    ) OR NOT EXISTS (
        SELECT 1 FROM table_game tg WHERE tg.table_code = t.table_code
    );

    -- Raise notices about skipped records if any
    IF invalid_games IS NOT NULL THEN
        RAISE NOTICE 'Skipped results with invalid game names: %', invalid_games;
    END IF;
    IF invalid_tables IS NOT NULL THEN
        RAISE NOTICE 'Skipped results with invalid table codes: %', invalid_tables;
    END IF;

    INSERT INTO game_result AS gr(result_code, game_name, table_code, result_time, outcome_description)
    SELECT 
        result_code,
        game_name,
        table_code,
        result_time::TIMESTAMP,
        outcome_description
    FROM tmp_game_result
    ON CONFLICT (result_code) DO UPDATE
    SET game_name = EXCLUDED.game_name,
        table_code = EXCLUDED.table_code,
        result_time = EXCLUDED.result_time,
        outcome_description = EXCLUDED.outcome_description
    WHERE gr.game_name IS DISTINCT FROM EXCLUDED.game_name
       OR gr.table_code IS DISTINCT FROM EXCLUDED.table_code
       OR gr.result_time IS DISTINCT FROM EXCLUDED.result_time
       OR gr.outcome_description IS DISTINCT FROM EXCLUDED.outcome_description;

    DROP TABLE tmp_game_result;
END;
$$;

SELECT * FROM load_game_result_from_csv('/csvs/game_result.csv');

SELECT * FROM game_result;