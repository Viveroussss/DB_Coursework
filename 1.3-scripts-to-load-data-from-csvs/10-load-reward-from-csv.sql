-- ============================
-- Function: load_reward_from_csv
-- ============================
CREATE OR REPLACE FUNCTION load_reward_from_csv(csv_path TEXT)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    CREATE TEMP TABLE tmp_reward (
        reward_code TEXT,
        name TEXT,
        points_required TEXT,
        description TEXT
    );

    EXECUTE format('COPY tmp_reward FROM %L CSV HEADER', csv_path);

    IF EXISTS (
        SELECT 1 FROM tmp_reward 
        WHERE reward_code IS NULL OR name IS NULL 
           OR points_required !~ '^\d+$'
    ) THEN
        RAISE EXCEPTION 'Validation failed in tmp_reward: required fields missing or invalid points format.';
    END IF;

    INSERT INTO reward AS rc(reward_code, name, points_required, description)
    SELECT 
        reward_code,
        name,
        points_required::INT,
        description
    FROM tmp_reward
    ON CONFLICT (reward_code) DO UPDATE
    SET name = EXCLUDED.name,
        points_required = EXCLUDED.points_required,
        description = EXCLUDED.description
    WHERE rc.name IS DISTINCT FROM EXCLUDED.name
       OR rc.points_required IS DISTINCT FROM EXCLUDED.points_required
       OR rc.description IS DISTINCT FROM EXCLUDED.description;

    DROP TABLE tmp_reward;
END;
$$;

SELECT * FROM load_reward_from_csv('/csvs/reward.csv');

SELECT * FROM reward;