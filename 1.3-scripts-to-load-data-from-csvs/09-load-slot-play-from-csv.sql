-- ============================
-- Function: load_slot_play_from_csv
-- ============================
CREATE OR REPLACE FUNCTION load_slot_play_from_csv(csv_path TEXT)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    CREATE TEMP TABLE tmp_slot_play (
        machine_code TEXT,
        player_email TEXT,
        play_time TEXT,
        bet_amount TEXT,
        win_amount TEXT
    );

    EXECUTE format('COPY tmp_slot_play FROM %L CSV HEADER', csv_path);

    IF EXISTS (
        SELECT 1 FROM tmp_slot_play 
        WHERE machine_code IS NULL OR player_email IS NULL 
           OR play_time !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
           OR bet_amount !~ '^\d+(\.\d+)?$'
           OR win_amount !~ '^\d+(\.\d+)?$'
    ) THEN
        RAISE EXCEPTION 'Validation failed in tmp_slot_play: required fields missing or invalid format.';
    END IF;

    INSERT INTO slot_play AS sp(machine_code, player_email, play_time, bet_amount, win_amount)
    SELECT 
        machine_code,
        player_email,
        play_time::TIMESTAMP,
        bet_amount::NUMERIC(10,2),
        win_amount::NUMERIC(10,2)
    FROM tmp_slot_play
    ON CONFLICT (machine_code, player_email, play_time) DO UPDATE
    SET bet_amount = EXCLUDED.bet_amount,
        win_amount = EXCLUDED.win_amount
    WHERE sp.bet_amount IS DISTINCT FROM EXCLUDED.bet_amount
       OR sp.win_amount IS DISTINCT FROM EXCLUDED.win_amount;

    DROP TABLE tmp_slot_play;
END;
$$;

SELECT * FROM load_slot_play_from_csv('/csvs/slot_play.csv');

SELECT * FROM slot_play;