-- =====================
-- 🎲 Execute All ETL Processes
-- =====================

-- Start logging
DO $$
DECLARE
    v_log_id INTEGER;
BEGIN
    -- Log start of ETL process
    v_log_id := log_etl_operation('execute_all_etl', 'STARTED');
    
    -- Execute initial data load
    RAISE NOTICE 'Starting initial data load...';
    PERFORM load_initial_data();
    
    -- Execute fact table loading
    RAISE NOTICE 'Loading fact tables...';
    PERFORM process_gaming_activity();
    PERFORM process_financial_transactions();
    PERFORM process_daily_gaming_summary();
    
    -- Log completion
    PERFORM log_etl_operation('execute_all_etl', 'COMPLETED', 1);
    
    RAISE NOTICE 'ETL process completed successfully!';
EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        PERFORM log_etl_operation(
            'execute_all_etl',
            'ERROR',
            0,
            SQLERRM
        );
        RAISE;
END;
$$; 