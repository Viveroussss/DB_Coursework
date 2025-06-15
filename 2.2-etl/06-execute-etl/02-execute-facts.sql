-- =====================
-- 📊 Execute Fact Table ETL
-- =====================

-- Execute gaming activity fact ETL
SELECT load_gaming_activity();

-- Execute financial transactions fact ETL
SELECT load_financial_transactions();

-- Execute daily summary aggregation
SELECT load_daily_gaming_summary(); 