-- ============================
-- Function: load_login_history_from_csv
-- ============================
CREATE OR REPLACE FUNCTION load_login_history_from_csv(csv_path TEXT)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    invalid_players TEXT;
    invalid_records RECORD;
BEGIN
    CREATE TEMP TABLE tmp_login_history (
        login_id TEXT,
        player_email TEXT,
        login_time TEXT,
        ip_address TEXT,
        device TEXT
    );

    EXECUTE format('COPY tmp_login_history FROM %L CSV HEADER', csv_path);

    -- Debug: Show records with missing required fields
    FOR invalid_records IN 
        SELECT * FROM tmp_login_history 
        WHERE login_id IS NULL OR player_email IS NULL 
           OR login_time IS NULL OR ip_address IS NULL
    LOOP
        RAISE NOTICE 'Record with missing required fields: %', invalid_records;
    END LOOP;

    -- Debug: Show records with invalid date format
    FOR invalid_records IN 
        SELECT * FROM tmp_login_history 
        WHERE login_time !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
    LOOP
        RAISE NOTICE 'Record with invalid date format: %', invalid_records;
    END LOOP;

    -- Remove invalid records instead of raising exception
    DELETE FROM tmp_login_history 
    WHERE login_id IS NULL 
       OR player_email IS NULL 
       OR login_time IS NULL 
       OR ip_address IS NULL
       OR login_time !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$';

    -- Check for invalid players and collect them for warning
    SELECT string_agg(DISTINCT t.player_email, ', ')
    INTO invalid_players
    FROM tmp_login_history t
    LEFT JOIN player p ON t.player_email = p.email
    WHERE p.email IS NULL;

    -- Remove records with invalid players
    DELETE FROM tmp_login_history t
    WHERE NOT EXISTS (
        SELECT 1 FROM player p WHERE p.email = t.player_email
    );

    -- Raise notice about skipped records if any
    IF invalid_players IS NOT NULL THEN
        RAISE NOTICE 'Skipped logins with invalid player emails: %', invalid_players;
    END IF;

    INSERT INTO login_history AS lh(login_id, player_email, login_time, ip_address, device)
    SELECT 
        login_id::UUID,
        player_email,
        login_time::TIMESTAMP,
        ip_address,
        LEFT(device, 100)  -- Truncate device to 100 characters
    FROM tmp_login_history
    ON CONFLICT (login_id) DO UPDATE
    SET player_email = EXCLUDED.player_email,
        login_time = EXCLUDED.login_time,
        ip_address = EXCLUDED.ip_address,
        device = EXCLUDED.device
    WHERE lh.player_email IS DISTINCT FROM EXCLUDED.player_email
       OR lh.login_time IS DISTINCT FROM EXCLUDED.login_time
       OR lh.ip_address IS DISTINCT FROM EXCLUDED.ip_address
       OR lh.device IS DISTINCT FROM EXCLUDED.device;

    DROP TABLE tmp_login_history;
END;
$$;

SELECT * FROM load_login_history_from_csv('/csvs/login_history.csv');

SELECT * FROM login_history;