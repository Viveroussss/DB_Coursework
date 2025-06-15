-- ============================
-- Function: load_game_from_csv (work)
-- ============================

CREATE OR REPLACE FUNCTION load_game_from_csv(file_path text)
RETURNS TABLE(inserted_count int, updated_count int) AS
$$
DECLARE
    total_rows int;
    filtered_rows int;
    record RECORD;
BEGIN
    -- Step 1: Create temporary table to load CSV
    CREATE TEMP TABLE tmp_game (
        game_name TEXT,
        type TEXT,
        min_bet TEXT,
        max_bet TEXT
    ) ON COMMIT DROP;

    -- Step 2: Load CSV file into temp table
    EXECUTE format('COPY tmp_game FROM %L WITH (FORMAT csv, HEADER true)', file_path);

    SELECT COUNT(*) INTO total_rows FROM tmp_game;
    RAISE NOTICE 'Total rows loaded: %', total_rows;

    RAISE NOTICE 'Loaded distinct type values (raw):';
    FOR record IN SELECT DISTINCT type FROM tmp_game LOOP
        RAISE NOTICE '- [%]', record.type;
    END LOOP;

    DROP TABLE IF EXISTS dedup;

    -- Step 3: Deduplicate, trim and normalize 'type' with mapping
    CREATE TEMP TABLE dedup AS
    SELECT DISTINCT ON (LOWER(TRIM(game_name)))
        TRIM(game_name) AS game_name,
        CASE
            WHEN INITCAP(TRIM(type)) IN ('Slot', 'Slots') THEN 'Slot'
            WHEN INITCAP(TRIM(type)) = 'Poker' THEN 'Poker'
            WHEN INITCAP(TRIM(type)) = 'Roulette' THEN 'Roulette'
            WHEN INITCAP(TRIM(type)) = 'Blackjack' THEN 'Blackjack'
            WHEN INITCAP(TRIM(type)) = 'Bingo' THEN 'Bingo'
            ELSE NULL
        END AS type,
        CASE WHEN min_bet ~ '^\d+(\.\d+)?$' THEN min_bet::NUMERIC(10,2) ELSE NULL END AS min_bet,
        CASE WHEN max_bet ~ '^\d+(\.\d+)?$' THEN max_bet::NUMERIC(10,2) ELSE NULL END AS max_bet
    FROM tmp_game
    WHERE
        TRIM(game_name) IS NOT NULL AND TRIM(game_name) <> ''
        AND TRIM(type) IS NOT NULL AND TRIM(type) <> ''
    ORDER BY LOWER(TRIM(game_name)), min_bet::NUMERIC(10,2) DESC;

    SELECT COUNT(*) INTO filtered_rows FROM dedup WHERE type IS NOT NULL;
    RAISE NOTICE 'Filtered valid rows to insert (type IS NOT NULL): %', filtered_rows;

    FOR record IN SELECT * FROM dedup WHERE type IS NOT NULL LOOP
        RAISE NOTICE 'Row to upsert: %, %, %, %', record.game_name, record.type, record.min_bet, record.max_bet;
    END LOOP;

    RETURN QUERY
    WITH upsert AS (
        INSERT INTO game(game_name, type, min_bet, max_bet)
        SELECT game_name, type, min_bet, max_bet FROM dedup WHERE type IS NOT NULL
        ON CONFLICT (game_name) DO UPDATE SET
            type = EXCLUDED.type,
            min_bet = EXCLUDED.min_bet,
            max_bet = EXCLUDED.max_bet
        WHERE
            game.type IS DISTINCT FROM EXCLUDED.type OR
            game.min_bet IS DISTINCT FROM EXCLUDED.min_bet OR
            game.max_bet IS DISTINCT FROM EXCLUDED.max_bet
        RETURNING xmax = 0 AS inserted
    )
    SELECT
        COUNT(*) FILTER (WHERE inserted)::INT,
        COUNT(*) FILTER (WHERE NOT inserted)::INT
    FROM upsert;
END;
$$ LANGUAGE plpgsql;



SELECT * FROM load_game_from_csv('/csvs/game.csv');

SELECT * FROM game;