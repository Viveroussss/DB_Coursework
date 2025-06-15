-- =====================
-- 🧑‍💼 Player
-- =====================
CREATE TABLE IF NOT EXISTS player (
    email VARCHAR(100) PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    dob DATE NOT NULL,
    phone VARCHAR(20),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loyalty_points INT DEFAULT 0
);

-- =====================
-- 👩‍💼 Staff
-- =====================
CREATE TABLE IF NOT EXISTS staff (
    staff_email VARCHAR(100) PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    position VARCHAR(50) NOT NULL,
    hire_date DATE NOT NULL,
    salary NUMERIC(10,2) NOT NULL
);

-- =====================
-- 🏛 Casino Location
-- =====================
CREATE TABLE IF NOT EXISTS casino_location (
    location_code VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    address TEXT NOT NULL,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50)
);

-- =====================
-- 🎮 Game
-- =====================
CREATE TABLE IF NOT EXISTS game (
    game_name VARCHAR(100) PRIMARY KEY,
    type VARCHAR(20) NOT NULL CHECK (type IN ('Table', 'Slot', 'Bingo', 'Roulette')),
    min_bet NUMERIC(10,2),
    max_bet NUMERIC(10,2)
);

ALTER TABLE game DROP CONSTRAINT game_type_check;

ALTER TABLE game ADD CONSTRAINT game_type_check
CHECK (type IN ('Slot', 'Poker', 'Roulette', 'Blackjack', 'Number', 'Bingo'));


-- =====================
-- 📊 Player-Game (M:N)
-- =====================
CREATE TABLE IF NOT EXISTS player_game (
    player_email VARCHAR(100) REFERENCES player(email),
    game_name VARCHAR(100) REFERENCES game(game_name),
    play_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    amount_bet NUMERIC(10,2),
    amount_won NUMERIC(10,2),
    PRIMARY KEY (player_email, game_name, play_time)
);

-- =====================
-- 🎰 Table Game
-- =====================
CREATE TABLE IF NOT EXISTS table_game (
    table_code VARCHAR(20) PRIMARY KEY,
    game_name VARCHAR(100) REFERENCES game(game_name),
    location_code VARCHAR(20) REFERENCES casino_location(location_code),
    status VARCHAR(20) CHECK (status IN ('Available', 'In Use', 'Maintenance')) DEFAULT 'Available'
);

-- =====================
-- 🧑‍🏫 Staff-Table Assignment (M:N)
-- =====================
CREATE TABLE IF NOT EXISTS staff_assigned_tables (
    staff_email VARCHAR(100) REFERENCES staff(staff_email),
    table_code VARCHAR(20) REFERENCES table_game(table_code),
    shift_start TIMESTAMP NOT NULL,
    shift_end TIMESTAMP NOT NULL,
    PRIMARY KEY (staff_email, table_code, shift_start)
);

-- =====================
-- 🕹 Slot Machine
-- =====================
CREATE TABLE IF NOT EXISTS slot_machine (
    machine_code VARCHAR(20) PRIMARY KEY,
    location_code VARCHAR(20) REFERENCES casino_location(location_code),
    status VARCHAR(20) CHECK (status IN ('Online', 'Offline', 'Maintenance')) DEFAULT 'Online',
    model VARCHAR(100)
);

-- =====================
-- 🔁 Slot Play
-- =====================
CREATE TABLE IF NOT EXISTS slot_play (
    machine_code VARCHAR(20) REFERENCES slot_machine(machine_code),
    player_email VARCHAR(100) REFERENCES player(email),
    play_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bet_amount NUMERIC(10,2),
    win_amount NUMERIC(10,2),
    PRIMARY KEY (machine_code, player_email, play_time)
);

-- =====================
-- 🎁 Reward Catalog
-- =====================
CREATE TABLE IF NOT EXISTS reward (
    reward_code VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    points_required INT NOT NULL,
    description TEXT
);

-- =====================
-- 🎁 Player Rewards
-- =====================
CREATE TABLE IF NOT EXISTS player_reward (
    player_email VARCHAR(100) REFERENCES player(email),
    reward_code VARCHAR(20) REFERENCES reward(reward_code),
    redeem_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (player_email, reward_code, redeem_date)
);

-- =====================
-- 💵 Transactions
-- =====================
CREATE TABLE IF NOT EXISTS transaction (
    transaction_code VARCHAR(30) PRIMARY KEY,
    player_email VARCHAR(100) REFERENCES player(email),
    amount NUMERIC(10,2) NOT NULL,
    transaction_type VARCHAR(20) CHECK (transaction_type IN ('Bet', 'Win', 'Deposit', 'Withdraw')),
    transaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    game_name VARCHAR(100) REFERENCES game(game_name)
);

-- =====================
-- 🧾 Game Result
-- =====================
CREATE TABLE IF NOT EXISTS game_result (
    result_code VARCHAR(30) PRIMARY KEY,
    game_name VARCHAR(100) NOT NULL REFERENCES game(game_name),
    table_code VARCHAR(20) REFERENCES table_game(table_code),
    result_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    outcome_description TEXT
);

-- =====================
-- 🔐 Login History
-- =====================
CREATE TABLE IF NOT EXISTS login_history (
    login_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_email VARCHAR(100) REFERENCES player(email),
    login_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ip_address VARCHAR(45),
    device VARCHAR(100)
);

-- =====================
-- 📝 Audit Log
-- =====================
CREATE TABLE IF NOT EXISTS audit_log (
    log_code VARCHAR(30) PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    performed_by VARCHAR(100) NOT NULL REFERENCES staff(staff_email),
    details TEXT
);

-- =====================
-- 📈 Indexes
-- =====================
CREATE INDEX IF NOT EXISTS idx_player_game_email ON player_game(player_email);
CREATE INDEX IF NOT EXISTS idx_player_game_game ON player_game(game_name);
CREATE INDEX IF NOT EXISTS idx_slot_play_machine ON slot_play(machine_code);
CREATE INDEX IF NOT EXISTS idx_slot_play_player ON slot_play(player_email);
CREATE INDEX IF NOT EXISTS idx_login_history_player ON login_history(player_email);
CREATE INDEX IF NOT EXISTS idx_table_game_location ON table_game(location_code);
