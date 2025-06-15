-- =====================
-- 📈 Indexes
-- =====================

-- Time Dimension Indexes
CREATE INDEX IF NOT EXISTS idx_dim_time_date ON dim_time(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_time_year ON dim_time(year);
CREATE INDEX IF NOT EXISTS idx_dim_time_month ON dim_time(month_number);

-- Game Dimension Indexes
CREATE INDEX IF NOT EXISTS idx_dim_game_name ON dim_game(game_name);
CREATE INDEX IF NOT EXISTS idx_dim_game_type ON dim_game(game_type);
CREATE INDEX IF NOT EXISTS idx_dim_game_active ON dim_game(is_active);

-- Slot Machine Dimension Indexes
CREATE INDEX IF NOT EXISTS idx_dim_slot_machine_code ON dim_slot_machine(machine_code);
CREATE INDEX IF NOT EXISTS idx_dim_slot_machine_active ON dim_slot_machine(is_active);

-- Location Dimension Indexes
CREATE INDEX IF NOT EXISTS idx_dim_location_code ON dim_location(location_code);
CREATE INDEX IF NOT EXISTS idx_dim_location_city ON dim_location(city);
CREATE INDEX IF NOT EXISTS idx_dim_location_country ON dim_location(country);

-- Player Dimension Indexes
CREATE INDEX IF NOT EXISTS idx_dim_player_key ON dim_player(player_key);
CREATE INDEX IF NOT EXISTS idx_dim_player_current ON dim_player(is_current);
CREATE INDEX IF NOT EXISTS idx_dim_player_loyalty ON dim_player(loyalty_tier);
CREATE INDEX IF NOT EXISTS idx_dim_player_age ON dim_player(age_group);

-- Staff Dimension Indexes
CREATE INDEX IF NOT EXISTS idx_dim_staff_key ON dim_staff(staff_key);
CREATE INDEX IF NOT EXISTS idx_dim_staff_current ON dim_staff(is_active);
CREATE INDEX IF NOT EXISTS idx_dim_staff_department ON dim_staff(department);

-- Device Dimension Indexes
CREATE INDEX IF NOT EXISTS idx_dim_device_type ON dim_device(device_type);
CREATE INDEX IF NOT EXISTS idx_dim_device_mobile ON dim_device(is_mobile);
CREATE INDEX IF NOT EXISTS idx_dim_device_desktop ON dim_device(is_desktop);

-- Bridge Table Indexes
CREATE INDEX IF NOT EXISTS idx_bridge_game_player_player ON bridge_game_player(player_id);
CREATE INDEX IF NOT EXISTS idx_bridge_game_player_game ON bridge_game_player(game_id);
CREATE INDEX IF NOT EXISTS idx_bridge_game_player_dates ON bridge_game_player(first_play_date, last_play_date);

-- Gaming Activity Fact Table Indexes
CREATE INDEX IF NOT EXISTS idx_fact_gaming_activity_time ON fact_gaming_activity(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_gaming_activity_player ON fact_gaming_activity(player_id);
CREATE INDEX IF NOT EXISTS idx_fact_gaming_activity_game ON fact_gaming_activity(game_id);
CREATE INDEX IF NOT EXISTS idx_fact_gaming_activity_location ON fact_gaming_activity(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_gaming_activity_staff ON fact_gaming_activity(staff_id);
CREATE INDEX IF NOT EXISTS idx_fact_gaming_activity_device ON fact_gaming_activity(device_id);
CREATE INDEX IF NOT EXISTS idx_fact_gaming_activity_result ON fact_gaming_activity(net_result);

-- Financial Transactions Fact Table Indexes
CREATE INDEX IF NOT EXISTS idx_fact_financial_time ON fact_financial_transactions(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_financial_player ON fact_financial_transactions(player_id);
CREATE INDEX IF NOT EXISTS idx_fact_financial_location ON fact_financial_transactions(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_financial_staff ON fact_financial_transactions(staff_id);
CREATE INDEX IF NOT EXISTS idx_fact_financial_type ON fact_financial_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_fact_financial_amount ON fact_financial_transactions(amount);

-- Daily Summary Indexes
CREATE INDEX IF NOT EXISTS idx_agg_daily_time ON agg_daily_gaming_summary(time_id);
CREATE INDEX IF NOT EXISTS idx_agg_daily_location ON agg_daily_gaming_summary(location_id);
CREATE INDEX IF NOT EXISTS idx_agg_daily_game ON agg_daily_gaming_summary(game_id);
CREATE INDEX IF NOT EXISTS idx_agg_daily_revenue ON agg_daily_gaming_summary(net_revenue); 