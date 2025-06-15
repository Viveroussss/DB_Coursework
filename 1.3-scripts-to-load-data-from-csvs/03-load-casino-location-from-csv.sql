-- ===============================
-- Function: load_casino_location_from_csv (work)
-- ===============================

CREATE OR REPLACE FUNCTION load_casino_location_from_csv(p_path TEXT)
RETURNS TABLE(inserted_count INTEGER, updated_count INTEGER) AS
$$
DECLARE
BEGIN
    CREATE TEMP TABLE tmp_casino_location (
        location_code TEXT,
        name TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        country TEXT
    ) ON COMMIT DROP;

    EXECUTE FORMAT('COPY tmp_casino_location FROM %L CSV HEADER', p_path);

    RETURN QUERY
    WITH dedup AS (
        SELECT DISTINCT ON (location_code)
            location_code,
            name,
            address,
            city,
            state,
            country
        FROM tmp_casino_location
        WHERE location_code IS NOT NULL AND location_code <> ''
          AND name IS NOT NULL AND name <> ''
        ORDER BY location_code
    ), upsert AS (
        INSERT INTO casino_location(location_code, name, address, city, state, country)
        SELECT location_code, name, address, city, state, country FROM dedup
        ON CONFLICT (location_code) DO UPDATE SET
            name = EXCLUDED.name,
            address = EXCLUDED.address,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            country = EXCLUDED.country
        WHERE
            casino_location.name IS DISTINCT FROM EXCLUDED.name OR
            casino_location.address IS DISTINCT FROM EXCLUDED.address OR
            casino_location.city IS DISTINCT FROM EXCLUDED.city OR
            casino_location.state IS DISTINCT FROM EXCLUDED.state OR
            casino_location.country IS DISTINCT FROM EXCLUDED.country
        RETURNING xmax = 0 AS inserted
    )
    SELECT
        COUNT(*) FILTER (WHERE inserted)::INT AS inserted_count,
        COUNT(*) FILTER (WHERE NOT inserted)::INT AS updated_count
    FROM upsert;

END;
$$ LANGUAGE plpgsql;





SELECT * FROM load_casino_location_from_csv('/csvs/casino_location.csv');

SELECT * FROM casino_location;