-- ============================
-- Function: load_table_game_from_csv
-- ============================
CREATE OR REPLACE FUNCTION load_table_game_from_csv(csv_path TEXT)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    invalid_games TEXT;
BEGIN
    CREATE TEMP TABLE tmp_table_game (
        table_code TEXT,
        game_name TEXT,
        location_code TEXT,
        status TEXT
    );

    EXECUTE format('COPY tmp_table_game FROM %L CSV HEADER', csv_path);

    IF EXISTS (
        SELECT 1 FROM tmp_table_game 
        WHERE table_code IS NULL OR game_name IS NULL OR location_code IS NULL
           OR status NOT IN ('Available', 'In Use', 'Maintenance')
    ) THEN
        RAISE EXCEPTION 'Validation failed in tmp_table_game: required fields missing or invalid status.';
    END IF;

    -- Check for invalid games and collect them for warning
    SELECT string_agg(DISTINCT t.game_name, ', ')
    INTO invalid_games
    FROM tmp_table_game t
    LEFT JOIN game g ON t.game_name = g.game_name
    WHERE g.game_name IS NULL;

    -- Remove records with invalid games
    DELETE FROM tmp_table_game t
    WHERE NOT EXISTS (
        SELECT 1 FROM game g WHERE g.game_name = t.game_name
    );

    -- Check if all location_code values exist in casino_location table
    IF EXISTS (
        SELECT DISTINCT t.location_code 
        FROM tmp_table_game t
        LEFT JOIN casino_location cl ON t.location_code = cl.location_code
        WHERE cl.location_code IS NULL
    ) THEN
        RAISE EXCEPTION 'Validation failed: Some location_code values do not exist in casino_location table.';
    END IF;

    -- Raise notice about skipped games if any
    IF invalid_games IS NOT NULL THEN
        RAISE NOTICE 'Skipped tables with invalid game names: %', invalid_games;
    END IF;

    INSERT INTO table_game AS tg(table_code, game_name, location_code, status)
    SELECT 
        table_code,
        game_name,
        location_code,
        COALESCE(status, 'Available')
    FROM tmp_table_game
    ON CONFLICT (table_code) DO UPDATE
    SET game_name = EXCLUDED.game_name,
        location_code = EXCLUDED.location_code,
        status = EXCLUDED.status
    WHERE tg.game_name IS DISTINCT FROM EXCLUDED.game_name
       OR tg.location_code IS DISTINCT FROM EXCLUDED.location_code
       OR tg.status IS DISTINCT FROM EXCLUDED.status;

    DROP TABLE tmp_table_game;
END;
$$;

SELECT * FROM load_table_game_from_csv('/csvs/table_game.csv');

SELECT * FROM table_game