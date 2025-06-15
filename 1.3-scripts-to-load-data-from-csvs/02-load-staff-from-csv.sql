-- ============================
-- Function: load_staff_from_csv (work)
-- ============================

CREATE OR REPLACE FUNCTION load_staff_from_csv(file_path TEXT)
RETURNS TABLE(inserted_count INT, updated_count INT) AS $$
DECLARE
BEGIN
    -- Step 1: Create and load temp table
    CREATE TEMP TABLE tmp_staff (
        staff_email TEXT,
        first_name TEXT,
        last_name TEXT,
        position TEXT,
        hire_date TEXT,
        salary TEXT
    ) ON COMMIT DROP;

    EXECUTE format(
        'COPY tmp_staff FROM %L WITH (FORMAT CSV, HEADER)',
        file_path
    );

    -- Step 2: Remove invalid or duplicate rows
    DELETE FROM tmp_staff
    WHERE staff_email IS NULL OR staff_email = ''
       OR last_name IS NULL OR last_name = ''
       OR hire_date !~ '^\d{4}-\d{2}-\d{2}$'
       OR salary !~ '^\d+(\.\d+)?$';

    DELETE FROM tmp_staff t1
    USING tmp_staff t2
    WHERE t1.ctid < t2.ctid
      AND t1.staff_email = t2.staff_email;

    -- Step 3: Insert/update with change detection
    RETURN QUERY
    WITH upsert AS (
        INSERT INTO staff(staff_email, first_name, last_name, position, hire_date, salary)
        SELECT
            staff_email,
            first_name,
            last_name,
            position,
            hire_date::DATE,
            salary::NUMERIC(10,2)
        FROM tmp_staff
        ON CONFLICT (staff_email) DO UPDATE
        SET first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            position = EXCLUDED.position,
            hire_date = EXCLUDED.hire_date,
            salary = EXCLUDED.salary
        WHERE staff.first_name IS DISTINCT FROM EXCLUDED.first_name OR
              staff.last_name IS DISTINCT FROM EXCLUDED.last_name OR
              staff.position IS DISTINCT FROM EXCLUDED.position OR
              staff.hire_date IS DISTINCT FROM EXCLUDED.hire_date OR
              staff.salary IS DISTINCT FROM EXCLUDED.salary
        RETURNING xmax = 0 AS inserted
    )
    SELECT
        COUNT(*) FILTER (WHERE inserted)::INT AS inserted_count,
        COUNT(*) FILTER (WHERE NOT inserted)::INT AS updated_count
    FROM upsert;
END;
$$ LANGUAGE plpgsql;





SELECT load_staff_from_csv('/csvs/staff.csv');

SELECT * FROM staff;