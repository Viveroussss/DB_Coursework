-- ============================
-- Function: load_player_from_csv (work)
-- ============================

CREATE OR REPLACE FUNCTION load_player_from_csv(file_path TEXT)
RETURNS TABLE(
    total_rows INT,
    inserted_rows INT,
    updated_rows INT,
    skipped_rows INT
) AS $$
DECLARE
    v_inserted INT := 0;
    v_updated INT := 0;
    v_skipped INT := 0;
BEGIN
    -- Create temporary staging table
    CREATE TEMP TABLE tmp_player (
        email TEXT,
        first_name TEXT,
        last_name TEXT,
        dob TEXT,
        phone TEXT,
        registration_date TEXT,
        loyalty_points TEXT
    ) ON COMMIT DROP;

    -- Load data from CSV
    EXECUTE FORMAT(
        'COPY tmp_player FROM %L WITH CSV HEADER',
        '/csvs/player.csv'
    );

    SELECT COUNT(*) INTO total_rows FROM tmp_player;

    -- Count skipped rows (missing email or last_name)
    SELECT COUNT(*) INTO v_skipped
    FROM tmp_player
    WHERE email IS NULL OR email = ''
       OR last_name IS NULL OR last_name = '';

    -- Upsert with deduplication by email
    WITH cleaned_data AS (
        SELECT DISTINCT ON (email)
            email,
            first_name,
            last_name,
            dob::DATE,
            phone,
            registration_date::TIMESTAMP,
            loyalty_points::INT
        FROM tmp_player
        WHERE email IS NOT NULL AND email <> ''
          AND last_name IS NOT NULL AND last_name <> ''
          AND dob ~ '^\d{4}-\d{2}-\d{2}$'
          AND registration_date ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
          AND loyalty_points ~ '^\d+$'
        ORDER BY email, registration_date::TIMESTAMP DESC NULLS LAST  -- pick most recent
    ),
    upsert AS (
        INSERT INTO player(email, first_name, last_name, dob, phone, registration_date, loyalty_points)
        SELECT email, first_name, last_name, dob, phone, registration_date, loyalty_points
        FROM cleaned_data
        ON CONFLICT (email) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            dob = EXCLUDED.dob,
            phone = EXCLUDED.phone,
            registration_date = EXCLUDED.registration_date,
            loyalty_points = EXCLUDED.loyalty_points
        WHERE
            player.first_name IS DISTINCT FROM EXCLUDED.first_name OR
            player.last_name IS DISTINCT FROM EXCLUDED.last_name OR
            player.dob IS DISTINCT FROM EXCLUDED.dob OR
            player.phone IS DISTINCT FROM EXCLUDED.phone OR
            player.registration_date IS DISTINCT FROM EXCLUDED.registration_date OR
            player.loyalty_points IS DISTINCT FROM EXCLUDED.loyalty_points
        RETURNING (xmax = 0) AS inserted
    )
    SELECT
        COUNT(*) FILTER (WHERE inserted) AS inserted_count,
        COUNT(*) FILTER (WHERE NOT inserted) AS updated_count
    INTO v_inserted, v_updated
    FROM upsert;

    RETURN QUERY SELECT total_rows, v_inserted, v_updated, v_skipped;
END;
$$ LANGUAGE plpgsql;





SELECT load_player_from_csv('/csvs/player.csv');

SELECT * FROM player;