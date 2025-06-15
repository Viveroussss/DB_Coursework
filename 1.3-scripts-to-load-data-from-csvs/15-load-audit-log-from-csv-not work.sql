-- ============================
-- Function: load_audit_log_from_csv (work)
-- ============================
CREATE OR REPLACE FUNCTION load_audit_log_from_csv(csv_path TEXT)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    invalid_staff TEXT;
    invalid_records RECORD;
    null_performed_by_count INTEGER;
BEGIN
    -- Drop and recreate the audit_log table to require performed_by
    DROP TABLE IF EXISTS audit_log CASCADE;
    CREATE TABLE audit_log (
        log_code VARCHAR(30) PRIMARY KEY,
        event_type VARCHAR(50) NOT NULL,
        event_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        performed_by VARCHAR(100) NOT NULL REFERENCES staff(staff_email),
        details TEXT
    );

    CREATE TEMP TABLE tmp_audit_log (
        log_code TEXT,
        event_type TEXT,
        event_time TEXT,
        performed_by TEXT,
        details TEXT
    );

    EXECUTE format('COPY tmp_audit_log FROM %L CSV HEADER', csv_path);

    -- Check for NULL performed_by values and raise error if found
    SELECT COUNT(*) INTO null_performed_by_count
    FROM tmp_audit_log
    WHERE performed_by IS NULL;

    IF null_performed_by_count > 0 THEN
        RAISE EXCEPTION 'Found % records with NULL performed_by. All audit logs must have a staff member assigned.', null_performed_by_count;
    END IF;

    -- Debug: Show records with missing required fields
    FOR invalid_records IN 
        SELECT * FROM tmp_audit_log 
        WHERE log_code IS NULL 
           OR event_type IS NULL 
           OR event_time IS NULL 
           OR details IS NULL
    LOOP
        RAISE NOTICE 'Record with missing required fields: %', invalid_records;
    END LOOP;

    -- Debug: Show records with invalid date format
    FOR invalid_records IN 
        SELECT * FROM tmp_audit_log 
        WHERE event_time !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
    LOOP
        RAISE NOTICE 'Record with invalid date format: %', invalid_records;
    END LOOP;

    -- Remove invalid records instead of raising exception
    DELETE FROM tmp_audit_log 
    WHERE log_code IS NULL 
       OR event_type IS NULL 
       OR event_time IS NULL 
       OR details IS NULL
       OR event_time !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$';

    -- Check for invalid staff emails and collect them for warning
    SELECT string_agg(DISTINCT t.performed_by, ', ')
    INTO invalid_staff
    FROM tmp_audit_log t
    LEFT JOIN staff s ON t.performed_by = s.staff_email
    WHERE s.staff_email IS NULL;

    -- Remove records with invalid staff emails
    DELETE FROM tmp_audit_log t
    WHERE NOT EXISTS (
        SELECT 1 FROM staff s WHERE s.staff_email = t.performed_by
    );

    -- Raise notice about skipped records if any
    IF invalid_staff IS NOT NULL THEN
        RAISE NOTICE 'Skipped audit logs with invalid staff emails: %', invalid_staff;
    END IF;

    INSERT INTO audit_log AS al(log_code, event_type, event_time, performed_by, details)
    SELECT 
        log_code,
        event_type,
        event_time::TIMESTAMP,
        performed_by,
        details
    FROM tmp_audit_log
    ON CONFLICT (log_code) DO UPDATE
    SET event_type = EXCLUDED.event_type,
        event_time = EXCLUDED.event_time,
        performed_by = EXCLUDED.performed_by,
        details = EXCLUDED.details
    WHERE al.event_type IS DISTINCT FROM EXCLUDED.event_type
       OR al.event_time IS DISTINCT FROM EXCLUDED.event_time
       OR al.performed_by IS DISTINCT FROM EXCLUDED.performed_by
       OR al.details IS DISTINCT FROM EXCLUDED.details;

    DROP TABLE tmp_audit_log;
END;
$$;

SELECT * FROM load_audit_log_from_csv('/csvs/audit_log.csv');

SELECT * FROM audit_log;