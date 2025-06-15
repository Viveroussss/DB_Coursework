-- =====================
-- 🎲 Casino Data Warehouse Schema
-- =====================

-- =====================
-- 📅 Time Dimension
-- =====================
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    day_of_month INTEGER,
    month_name VARCHAR(10),
    month_number INTEGER,
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- =====================
-- 🎮 Game Dimension
-- =====================
CREATE TABLE IF NOT EXISTS dim_game (
    game_id SERIAL PRIMARY KEY,
    game_name VARCHAR(100) NOT NULL,
    game_type VARCHAR(20) NOT NULL,
    min_bet NUMERIC(10,2),
    max_bet NUMERIC(10,2),
    is_active BOOLEAN DEFAULT true
);

-- =====================
-- 🎰 Slot Machine Dimension
-- =====================
CREATE TABLE IF NOT EXISTS dim_slot_machine (
    slot_machine_id SERIAL PRIMARY KEY,
    machine_code VARCHAR(20) NOT NULL,
    model VARCHAR(100),
    is_active BOOLEAN DEFAULT true
);

-- =====================
-- 🏛 Location Dimension
-- =====================
CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    location_code VARCHAR(20) NOT NULL,
    location_name VARCHAR(100) NOT NULL,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    is_active BOOLEAN DEFAULT true
);

-- =====================
-- 👥 Player Dimension (Type 2 SCD)
-- =====================
CREATE TABLE IF NOT EXISTS dim_player (
    player_id SERIAL PRIMARY KEY,
    player_key VARCHAR(100) NOT NULL,  -- Natural key (email)
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    age_group VARCHAR(20),
    registration_date DATE,
    loyalty_tier VARCHAR(20),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT true,
    version_number INTEGER DEFAULT 1
);

-- =====================
-- 👨‍💼 Staff Dimension (Type 2 SCD)
-- =====================
CREATE TABLE IF NOT EXISTS dim_staff (
    staff_id SERIAL PRIMARY KEY,
    staff_key VARCHAR(100) NOT NULL,  -- Natural key (email)
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    position VARCHAR(50),
    department VARCHAR(50),
    hire_date DATE,
    is_active BOOLEAN DEFAULT true,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    version_number INTEGER DEFAULT 1
);

-- =====================
-- 📱 Device Dimension
-- =====================
CREATE TABLE IF NOT EXISTS dim_device (
    device_id SERIAL PRIMARY KEY,
    device_type VARCHAR(50) NOT NULL,
    device_model VARCHAR(100),
    operating_system VARCHAR(50),
    browser VARCHAR(50),
    is_mobile BOOLEAN,
    is_tablet BOOLEAN,
    is_desktop BOOLEAN
);

-- =====================
-- 🎲 Game-Player Bridge Table
-- =====================
CREATE TABLE IF NOT EXISTS bridge_game_player (
    bridge_id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES dim_player(player_id),
    game_id INTEGER REFERENCES dim_game(game_id),
    first_play_date TIMESTAMP,
    last_play_date TIMESTAMP,
    total_plays INTEGER DEFAULT 0,
    total_bet_amount NUMERIC(15,2) DEFAULT 0,
    total_win_amount NUMERIC(15,2) DEFAULT 0
);

-- =====================
-- 💰 Gaming Activity Fact Table
-- =====================
CREATE TABLE IF NOT EXISTS fact_gaming_activity (
    activity_id SERIAL PRIMARY KEY,
    time_id INTEGER REFERENCES dim_time(time_id),
    player_id INTEGER REFERENCES dim_player(player_id),
    game_id INTEGER REFERENCES dim_game(game_id),
    location_id INTEGER REFERENCES dim_location(location_id),
    slot_machine_id INTEGER REFERENCES dim_slot_machine(slot_machine_id),
    staff_id INTEGER REFERENCES dim_staff(staff_id),
    device_id INTEGER REFERENCES dim_device(device_id),
    bet_amount NUMERIC(10,2),
    win_amount NUMERIC(10,2),
    net_result NUMERIC(10,2),
    session_duration_minutes INTEGER,
    plays_count INTEGER DEFAULT 1
);

-- =====================
-- 💵 Financial Transactions Fact Table
-- =====================
CREATE TABLE IF NOT EXISTS fact_financial_transactions (
    transaction_id SERIAL PRIMARY KEY,
    time_id INTEGER REFERENCES dim_time(time_id),
    player_id INTEGER REFERENCES dim_player(player_id),
    location_id INTEGER REFERENCES dim_location(location_id),
    staff_id INTEGER REFERENCES dim_staff(staff_id),
    transaction_type VARCHAR(20),
    amount NUMERIC(10,2),
    loyalty_points_earned INTEGER,
    loyalty_points_redeemed INTEGER
);

-- =====================
-- 📊 Aggregated Daily Gaming Summary
-- =====================
CREATE TABLE IF NOT EXISTS agg_daily_gaming_summary (
    summary_id SERIAL PRIMARY KEY,
    time_id INTEGER REFERENCES dim_time(time_id),
    location_id INTEGER REFERENCES dim_location(location_id),
    game_id INTEGER REFERENCES dim_game(game_id),
    total_players INTEGER,
    total_bets NUMERIC(15,2),
    total_wins NUMERIC(15,2),
    net_revenue NUMERIC(15,2),
    average_bet_amount NUMERIC(10,2),
    total_plays INTEGER
);