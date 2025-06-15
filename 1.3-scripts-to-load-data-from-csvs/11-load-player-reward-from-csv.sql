-- ============================
-- Function: load_player_reward_from_csv (work)
-- ============================
CREATE OR REPLACE FUNCTION load_player_reward_from_csv(csv_path TEXT)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    CREATE TEMP TABLE tmp_player_reward (
        player_email TEXT,
        reward_code TEXT,
        redeem_date TEXT
    );

    EXECUTE format('COPY tmp_player_reward FROM %L CSV HEADER', csv_path);

    IF EXISTS (
        SELECT 1 FROM tmp_player_reward 
        WHERE player_email IS NULL OR reward_code IS NULL 
           OR redeem_date !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
    ) THEN
        RAISE EXCEPTION 'Validation failed in tmp_player_reward: required fields missing or invalid date format.';
    END IF;

    INSERT INTO player_reward AS pr(player_email, reward_code, redeem_date)
    SELECT 
        player_email,
        reward_code,
        redeem_date::TIMESTAMP
    FROM tmp_player_reward
    ON CONFLICT (player_email, reward_code, redeem_date) DO UPDATE
    SET redeem_date = EXCLUDED.redeem_date
    WHERE pr.redeem_date IS DISTINCT FROM EXCLUDED.redeem_date;

    DROP TABLE tmp_player_reward;
END;
$$;

SELECT * FROM load_player_reward_from_csv('/csvs/player_reward.csv');

SELECT * FROM player_reward;