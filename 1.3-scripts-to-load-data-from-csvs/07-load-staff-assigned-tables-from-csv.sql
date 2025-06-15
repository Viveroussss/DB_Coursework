-- ===============================
-- Function: load_staff_assigned_tables_from_csv (work)
-- ===============================
CREATE OR REPLACE FUNCTION load_staff_assigned_tables_from_csv(csv_path TEXT)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    invalid_tables TEXT;
    invalid_staff TEXT;
BEGIN
    CREATE TEMP TABLE tmp_staff_assigned_tables (
        staff_email TEXT,
        table_code TEXT,
        shift_start TEXT,
        shift_end TEXT
    );

    EXECUTE format('COPY tmp_staff_assigned_tables FROM %L CSV HEADER', csv_path);

    IF EXISTS (
        SELECT 1 FROM tmp_staff_assigned_tables 
        WHERE staff_email IS NULL OR table_code IS NULL 
           OR shift_start !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
           OR shift_end !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
    ) THEN
        RAISE EXCEPTION 'Validation failed in tmp_staff_assigned_tables: required fields missing or invalid format.';
    END IF;

    -- Check for invalid table codes and collect them for warning
    SELECT string_agg(DISTINCT t.table_code, ', ')
    INTO invalid_tables
    FROM tmp_staff_assigned_tables t
    LEFT JOIN table_game tg ON t.table_code = tg.table_code
    WHERE tg.table_code IS NULL;

    -- Check for invalid staff emails and collect them for warning
    SELECT string_agg(DISTINCT t.staff_email, ', ')
    INTO invalid_staff
    FROM tmp_staff_assigned_tables t
    LEFT JOIN staff s ON t.staff_email = s.staff_email
    WHERE s.staff_email IS NULL;

    -- Remove records with invalid table codes or staff emails
    DELETE FROM tmp_staff_assigned_tables t
    WHERE NOT EXISTS (
        SELECT 1 FROM table_game tg WHERE tg.table_code = t.table_code
    ) OR NOT EXISTS (
        SELECT 1 FROM staff s WHERE s.staff_email = t.staff_email
    );

    -- Raise notices about skipped records if any
    IF invalid_tables IS NOT NULL THEN
        RAISE NOTICE 'Skipped assignments with invalid table codes: %', invalid_tables;
    END IF;
    IF invalid_staff IS NOT NULL THEN
        RAISE NOTICE 'Skipped assignments with invalid staff emails: %', invalid_staff;
    END IF;

    INSERT INTO staff_assigned_tables AS sat(staff_email, table_code, shift_start, shift_end)
    SELECT 
        staff_email,
        table_code,
        shift_start::TIMESTAMP,
        shift_end::TIMESTAMP
    FROM tmp_staff_assigned_tables
    ON CONFLICT (staff_email, table_code, shift_start) DO UPDATE
    SET shift_end = EXCLUDED.shift_end
    WHERE sat.shift_end IS DISTINCT FROM EXCLUDED.shift_end;

    DROP TABLE tmp_staff_assigned_tables;
END;
$$;

SELECT * FROM load_staff_assigned_tables_from_csv('/csvs/staff_assigned_tables.csv');

SELECT * FROM staff_assigned_tables;