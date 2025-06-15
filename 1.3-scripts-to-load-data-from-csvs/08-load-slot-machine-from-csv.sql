-- ============================
-- Function: load_slot_machine_from_csv
-- ============================
CREATE OR REPLACE FUNCTION load_slot_machine_from_csv(csv_path TEXT)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    invalid_locations TEXT;
    invalid_records RECORD;
BEGIN
    CREATE TEMP TABLE tmp_slot_machine (
        machine_code TEXT,
        location_code TEXT,
        status TEXT,
        model TEXT
    );

    EXECUTE format('COPY tmp_slot_machine FROM %L CSV HEADER', csv_path);

    -- Debug: Show records with missing required fields
    FOR invalid_records IN 
        SELECT * FROM tmp_slot_machine 
        WHERE machine_code IS NULL OR location_code IS NULL
    LOOP
        RAISE NOTICE 'Record with missing required fields: %', invalid_records;
    END LOOP;

    -- Debug: Show records with invalid status
    FOR invalid_records IN 
        SELECT * FROM tmp_slot_machine 
        WHERE status NOT IN ('Online', 'Offline', 'Maintenance')
    LOOP
        RAISE NOTICE 'Record with invalid status: %', invalid_records;
    END LOOP;

    -- Remove invalid records instead of raising exception
    DELETE FROM tmp_slot_machine 
    WHERE machine_code IS NULL 
       OR location_code IS NULL
       OR status NOT IN ('Online', 'Offline', 'Maintenance');

    -- Check for invalid locations and collect them for warning
    SELECT string_agg(DISTINCT t.location_code, ', ')
    INTO invalid_locations
    FROM tmp_slot_machine t
    LEFT JOIN casino_location cl ON t.location_code = cl.location_code
    WHERE cl.location_code IS NULL;

    -- Remove records with invalid locations
    DELETE FROM tmp_slot_machine t
    WHERE NOT EXISTS (
        SELECT 1 FROM casino_location cl WHERE cl.location_code = t.location_code
    );

    -- Raise notice about skipped locations if any
    IF invalid_locations IS NOT NULL THEN
        RAISE NOTICE 'Skipped machines with invalid location codes: %', invalid_locations;
    END IF;

    INSERT INTO slot_machine AS sm(machine_code, location_code, status, model)
    SELECT 
        machine_code,
        location_code,
        status,
        model
    FROM tmp_slot_machine
    ON CONFLICT (machine_code) DO UPDATE
    SET location_code = EXCLUDED.location_code,
        status = EXCLUDED.status,
        model = EXCLUDED.model
    WHERE sm.location_code IS DISTINCT FROM EXCLUDED.location_code
       OR sm.status IS DISTINCT FROM EXCLUDED.status
       OR sm.model IS DISTINCT FROM EXCLUDED.model;

    DROP TABLE tmp_slot_machine;
END;
$$;

SELECT * FROM load_slot_machine_from_csv('/csvs/slot_machine.csv');

SELECT * FROM slot_machine;