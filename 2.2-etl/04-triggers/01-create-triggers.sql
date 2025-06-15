-- =====================
-- 🔄 ETL Triggers
-- =====================

-- Dimension triggers
CREATE TRIGGER trg_player_dimension_update
AFTER INSERT OR UPDATE ON player
FOR EACH ROW
EXECUTE FUNCTION update_player_dimension();

CREATE TRIGGER trg_staff_dimension_update
AFTER INSERT OR UPDATE ON staff
FOR EACH ROW
EXECUTE FUNCTION update_staff_dimension();

CREATE TRIGGER trg_device_dimension_update
AFTER INSERT ON login_history
FOR EACH ROW
EXECUTE FUNCTION update_device_dimension();

-- Fact table triggers
CREATE TRIGGER trg_gaming_activity_update
AFTER INSERT ON player_game
FOR EACH ROW
EXECUTE FUNCTION update_gaming_activity();

CREATE TRIGGER trg_financial_transactions_update
AFTER INSERT ON transaction
FOR EACH ROW
EXECUTE FUNCTION update_financial_transactions();

CREATE TRIGGER trg_daily_summary_update
AFTER INSERT ON fact_gaming_activity
FOR EACH ROW
EXECUTE FUNCTION update_daily_gaming_summary(); 