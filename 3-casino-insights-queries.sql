-- ============================================
-- 🎰 CASINO DATA INSIGHTS QUERIES
-- ============================================
-- This script contains queries to analyze casino data
-- using both OLTP (operational) and OLAP (analytical) databases
-- ============================================

-- ============================================
-- 3.1 OLTP QUERIES (Operational Database)
-- ============================================

-- ============================================
-- Query 1: Which games are most popular by month and year?
-- Business Question: What are the most played games by time period?
-- ============================================

-- OLTP Query: Game popularity by month and year
SELECT 
    g.game_name,
    g.type as game_type,
    EXTRACT(YEAR FROM pg.play_time) as year,
    EXTRACT(MONTH FROM pg.play_time) as month,
    COUNT(*) as total_plays,
    SUM(pg.amount_bet) as total_bets,
    SUM(pg.amount_won) as total_wins,
    AVG(pg.amount_bet) as avg_bet_amount,
    ROUND((SUM(pg.amount_won) - SUM(pg.amount_bet))::numeric, 2) as net_revenue
FROM player_game pg
JOIN game g ON pg.game_name = g.game_name
WHERE pg.play_time >= '2023-01-01'
GROUP BY g.game_name, g.type, EXTRACT(YEAR FROM pg.play_time), EXTRACT(MONTH FROM pg.play_time)
ORDER BY year DESC, month DESC, total_plays DESC;

-- ============================================
-- Query 2: Player loyalty analysis and spending patterns
-- Business Question: Who are our most valuable players and what are their spending patterns?
-- ============================================

-- OLTP Query: Top players by loyalty points and spending
SELECT 
    p.email,
    p.first_name,
    p.last_name,
    p.loyalty_points,
    p.registration_date,
    COUNT(pg.play_time) as total_games_played,
    SUM(pg.amount_bet) as total_amount_bet,
    SUM(pg.amount_won) as total_amount_won,
    ROUND((SUM(pg.amount_won) - SUM(pg.amount_bet))::numeric, 2) as net_result,
    AVG(pg.amount_bet) as avg_bet_amount,
    COUNT(DISTINCT pg.game_name) as games_played_count,
    CASE 
        WHEN p.loyalty_points >= 8000 THEN 'Platinum'
        WHEN p.loyalty_points >= 5000 THEN 'Gold'
        WHEN p.loyalty_points >= 2000 THEN 'Silver'
        ELSE 'Bronze'
    END as loyalty_tier
FROM player p
LEFT JOIN player_game pg ON p.email = pg.player_email
GROUP BY p.email, p.first_name, p.last_name, p.loyalty_points, p.registration_date
HAVING COUNT(pg.play_time) > 0
ORDER BY p.loyalty_points DESC, total_amount_bet DESC
LIMIT 20;

-- ============================================
-- Query 3: Financial transaction analysis by type and time
-- Business Question: What are the patterns in deposits, withdrawals, bets, and wins?
-- ============================================

-- OLTP Query: Transaction analysis by type and time period
SELECT 
    t.transaction_type,
    EXTRACT(YEAR FROM t.transaction_time) as year,
    EXTRACT(MONTH FROM t.transaction_time) as month,
    EXTRACT(DOW FROM t.transaction_time) as day_of_week,
    COUNT(*) as transaction_count,
    SUM(t.amount) as total_amount,
    AVG(t.amount) as avg_amount,
    MIN(t.amount) as min_amount,
    MAX(t.amount) as max_amount,
    g.game_name,
    g.type as game_type
FROM transaction t
LEFT JOIN game g ON t.game_name = g.game_name
WHERE t.transaction_time >= '2023-01-01'
GROUP BY 
    t.transaction_type, 
    EXTRACT(YEAR FROM t.transaction_time), 
    EXTRACT(MONTH FROM t.transaction_time),
    EXTRACT(DOW FROM t.transaction_time),
    g.game_name,
    g.type
ORDER BY 
    t.transaction_type, 
    year DESC, 
    month DESC, 
    total_amount DESC;

-- ============================================
-- 3.2 OLAP QUERIES (Analytical Database)
-- ============================================

-- ============================================
-- Query 1: Gaming activity trends by time dimension
-- Business Question: How does gaming activity vary by time periods (day, month, quarter)?
-- ============================================

-- OLAP Query: Gaming activity trends with time dimension
SELECT 
    dt.year,
    dt.quarter,
    dt.month_name,
    dt.day_of_week,
    COUNT(DISTINCT fga.player_id) as unique_players,
    COUNT(fga.activity_id) as total_activities,
    SUM(fga.bet_amount) as total_bets,
    SUM(fga.win_amount) as total_wins,
    ROUND((SUM(fga.win_amount) - SUM(fga.bet_amount))::numeric, 2) as net_revenue,
    AVG(fga.bet_amount) as avg_bet_amount,
    SUM(fga.session_duration_minutes) as total_session_time,
    ROUND(AVG(fga.session_duration_minutes)::numeric, 2) as avg_session_duration
FROM fact_gaming_activity fga
JOIN dim_time dt ON fga.time_id = dt.time_id
WHERE dt.full_date >= '2023-01-01'
GROUP BY dt.year, dt.quarter, dt.month_name, dt.day_of_week
ORDER BY dt.year DESC, dt.quarter DESC, dt.month_name, dt.day_of_week;

-- ============================================
-- Query 2: Player segmentation and behavior analysis
-- Business Question: How can we segment players based on their behavior and value?
-- ============================================

-- OLAP Query: Player segmentation analysis
SELECT 
    dp.loyalty_tier,
    dp.age_group,
    COUNT(DISTINCT dp.player_id) as player_count,
    AVG(fga.bet_amount) as avg_bet_amount,
    SUM(fga.bet_amount) as total_bets,
    SUM(fga.win_amount) as total_wins,
    ROUND((SUM(fga.win_amount) - SUM(fga.bet_amount))::numeric, 2) as net_revenue,
    COUNT(fga.activity_id) as total_activities,
    ROUND(AVG(fga.session_duration_minutes)::numeric, 2) as avg_session_duration,
    COUNT(DISTINCT fga.game_id) as games_played_count,
    ROUND((SUM(fga.bet_amount) / COUNT(DISTINCT dp.player_id))::numeric, 2) as avg_player_bet
FROM fact_gaming_activity fga
JOIN dim_player dp ON fga.player_id = dp.player_id
WHERE dp.is_current = true
GROUP BY dp.loyalty_tier, dp.age_group
ORDER BY total_bets DESC, player_count DESC;

-- ============================================
-- Query 3: Game performance and profitability analysis
-- Business Question: Which games are most profitable and popular across different locations?
-- ============================================

-- OLAP Query: Game performance analysis with location dimension
SELECT 
    dg.game_name,
    dg.game_type,
    dl.location_name,
    dl.city,
    dl.state,
    COUNT(DISTINCT fga.player_id) as unique_players,
    COUNT(fga.activity_id) as total_plays,
    SUM(fga.bet_amount) as total_bets,
    SUM(fga.win_amount) as total_wins,
    ROUND((SUM(fga.win_amount) - SUM(fga.bet_amount))::numeric, 2) as net_revenue,
    ROUND((SUM(fga.win_amount) / NULLIF(SUM(fga.bet_amount), 0) * 100)::numeric, 2) as win_percentage,
    AVG(fga.bet_amount) as avg_bet_amount,
    ROUND((SUM(fga.bet_amount) / COUNT(fga.activity_id))::numeric, 2) as avg_bet_per_play
FROM fact_gaming_activity fga
JOIN dim_game dg ON fga.game_id = dg.game_id
JOIN dim_location dl ON fga.location_id = dl.location_id
WHERE dg.is_active = true
GROUP BY dg.game_name, dg.game_type, dl.location_name, dl.city, dl.state
ORDER BY net_revenue DESC, total_plays DESC;

-- ============================================
-- COMPARISON QUERIES: OLTP vs OLAP
-- ============================================

-- ============================================
-- Comparison Query 1: Monthly gaming revenue comparison
-- ============================================

-- OLTP Version: Monthly revenue calculation
SELECT 
    EXTRACT(YEAR FROM pg.play_time) as year,
    EXTRACT(MONTH FROM pg.play_time) as month,
    SUM(pg.amount_bet) as total_bets,
    SUM(pg.amount_won) as total_wins,
    ROUND((SUM(pg.amount_won) - SUM(pg.amount_bet))::numeric, 2) as net_revenue,
    COUNT(*) as total_plays
FROM player_game pg
WHERE pg.play_time >= '2023-01-01'
GROUP BY EXTRACT(YEAR FROM pg.play_time), EXTRACT(MONTH FROM pg.play_time)
ORDER BY year DESC, month DESC;

-- OLAP Version: Monthly revenue calculation (more efficient)
SELECT 
    dt.year,
    dt.month_number,
    dt.month_name,
    SUM(fga.bet_amount) as total_bets,
    SUM(fga.win_amount) as total_wins,
    ROUND((SUM(fga.win_amount) - SUM(fga.bet_amount))::numeric, 2) as net_revenue,
    COUNT(fga.activity_id) as total_plays
FROM fact_gaming_activity fga
JOIN dim_time dt ON fga.time_id = dt.time_id
WHERE dt.full_date >= '2023-01-01'
GROUP BY dt.year, dt.month_number, dt.month_name
ORDER BY dt.year DESC, dt.month_number DESC;

-- ============================================
-- Comparison Query 2: Top players comparison
-- ============================================

-- OLTP Version: Top players by total betting
SELECT 
    p.email,
    p.first_name,
    p.last_name,
    SUM(pg.amount_bet) as total_bet,
    SUM(pg.amount_won) as total_won,
    COUNT(pg.play_time) as total_plays
FROM player p
JOIN player_game pg ON p.email = pg.player_email
GROUP BY p.email, p.first_name, p.last_name
ORDER BY total_bet DESC
LIMIT 10;

-- OLAP Version: Top players by total betting (with additional dimensions)
SELECT 
    dp.first_name,
    dp.last_name,
    dp.loyalty_tier,
    dp.age_group,
    SUM(fga.bet_amount) as total_bet,
    SUM(fga.win_amount) as total_won,
    COUNT(fga.activity_id) as total_plays,
    ROUND(AVG(fga.session_duration_minutes)::numeric, 2) as avg_session_duration
FROM fact_gaming_activity fga
JOIN dim_player dp ON fga.player_id = dp.player_id
WHERE dp.is_current = true
GROUP BY dp.first_name, dp.last_name, dp.loyalty_tier, dp.age_group
ORDER BY total_bet DESC
LIMIT 10;

-- ============================================
-- Comparison Query 3: Game popularity comparison
-- ============================================

-- OLTP Version: Game popularity
SELECT 
    g.game_name,
    g.type,
    COUNT(pg.play_time) as total_plays,
    SUM(pg.amount_bet) as total_bets,
    AVG(pg.amount_bet) as avg_bet
FROM game g
JOIN player_game pg ON g.game_name = pg.game_name
GROUP BY g.game_name, g.type
ORDER BY total_plays DESC;

-- OLAP Version: Game popularity with location and time dimensions
SELECT 
    dg.game_name,
    dg.game_type,
    dl.location_name,
    COUNT(fga.activity_id) as total_plays,
    SUM(fga.bet_amount) as total_bets,
    AVG(fga.bet_amount) as avg_bet,
    COUNT(DISTINCT fga.player_id) as unique_players
FROM fact_gaming_activity fga
JOIN dim_game dg ON fga.game_id = dg.game_id
JOIN dim_location dl ON fga.location_id = dl.location_id
WHERE dg.is_active = true
GROUP BY dg.game_name, dg.game_type, dl.location_name
ORDER BY total_plays DESC;

-- ============================================
-- ADDITIONAL INSIGHTFUL QUERIES
-- ============================================

-- ============================================
-- Query 4: Peak gaming hours analysis
-- ============================================

-- OLTP Query: Gaming activity by hour of day
SELECT 
    EXTRACT(HOUR FROM pg.play_time) as hour_of_day,
    COUNT(*) as total_plays,
    SUM(pg.amount_bet) as total_bets,
    AVG(pg.amount_bet) as avg_bet_amount,
    COUNT(DISTINCT pg.player_email) as unique_players
FROM player_game pg
WHERE pg.play_time >= '2023-01-01'
GROUP BY EXTRACT(HOUR FROM pg.play_time)
ORDER BY hour_of_day;

-- OLAP Query: Gaming activity by hour with time dimension
SELECT 
    dt.hour_of_day,
    COUNT(fga.activity_id) as total_activities,
    SUM(fga.bet_amount) as total_bets,
    AVG(fga.bet_amount) as avg_bet_amount,
    COUNT(DISTINCT fga.player_id) as unique_players,
    ROUND(AVG(fga.session_duration_minutes)::numeric, 2) as avg_session_duration
FROM fact_gaming_activity fga
JOIN dim_time dt ON fga.time_id = dt.time_id
GROUP BY dt.hour_of_day
ORDER BY dt.hour_of_day;

-- ============================================
-- Query 5: Player retention analysis
-- ============================================

-- OLTP Query: Player retention by registration month
SELECT 
    EXTRACT(YEAR FROM p.registration_date) as reg_year,
    EXTRACT(MONTH FROM p.registration_date) as reg_month,
    COUNT(DISTINCT p.email) as registered_players,
    COUNT(DISTINCT pg.player_email) as active_players,
    ROUND((COUNT(DISTINCT pg.player_email)::numeric / COUNT(DISTINCT p.email) * 100)::numeric, 2) as retention_rate
FROM player p
LEFT JOIN player_game pg ON p.email = pg.player_email 
    AND pg.play_time >= p.registration_date + INTERVAL '30 days'
GROUP BY EXTRACT(YEAR FROM p.registration_date), EXTRACT(MONTH FROM p.registration_date)
ORDER BY reg_year DESC, reg_month DESC;

-- ============================================
-- Query 6: Financial health indicators
-- ============================================

-- OLTP Query: Monthly financial summary
SELECT 
    EXTRACT(YEAR FROM t.transaction_time) as year,
    EXTRACT(MONTH FROM t.transaction_time) as month,
    SUM(CASE WHEN t.transaction_type = 'Deposit' THEN t.amount ELSE 0 END) as total_deposits,
    SUM(CASE WHEN t.transaction_type = 'Withdraw' THEN t.amount ELSE 0 END) as total_withdrawals,
    SUM(CASE WHEN t.transaction_type = 'Bet' THEN t.amount ELSE 0 END) as total_bets,
    SUM(CASE WHEN t.transaction_type = 'Win' THEN t.amount ELSE 0 END) as total_wins,
    ROUND((SUM(CASE WHEN t.transaction_type = 'Win' THEN t.amount ELSE 0 END) - 
           SUM(CASE WHEN t.transaction_type = 'Bet' THEN t.amount ELSE 0 END))::numeric, 2) as gaming_revenue,
    COUNT(CASE WHEN t.transaction_type = 'Deposit' THEN 1 END) as deposit_count,
    COUNT(CASE WHEN t.transaction_type = 'Withdraw' THEN 1 END) as withdrawal_count
FROM transaction t
WHERE t.transaction_time >= '2023-01-01'
GROUP BY EXTRACT(YEAR FROM t.transaction_time), EXTRACT(MONTH FROM t.transaction_time)
ORDER BY year DESC, month DESC;

-- ============================================
-- END OF CASINO INSIGHTS QUERIES
-- ============================================
-- These queries provide comprehensive insights into:
-- 1. Game popularity and performance
-- 2. Player behavior and loyalty
-- 3. Financial transactions and revenue
-- 4. Time-based patterns and trends
-- 5. Location-based performance
-- 6. Player retention and engagement
-- ============================================