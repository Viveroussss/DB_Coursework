-- =====================
-- 👨‍💼 Staff Dimension ETL
-- =====================

-- Function to handle staff dimension (Type 2 SCD) with improved error handling
CREATE OR REPLACE FUNCTION update_staff_dimension()
RETURNS TRIGGER AS $$
DECLARE
    v_staff_id INTEGER;
    v_current_record RECORD;
    v_log_id INTEGER;
    v_department VARCHAR(50);
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('update_staff_dimension', 'STARTED');
    
    BEGIN
        -- Calculate department
        v_department := CASE 
            WHEN NEW.position LIKE '%Manager%' THEN 'Management'
            WHEN NEW.position LIKE '%Dealer%' THEN 'Gaming'
            WHEN NEW.position LIKE '%Security%' THEN 'Security'
            ELSE 'Operations'
        END;
        
        -- Check if staff exists in dimension
        SELECT staff_id, is_current INTO v_current_record
        FROM dim_staff
        WHERE staff_key = NEW.staff_email
        AND is_current = TRUE;
        
        -- If staff doesn't exist, create new record
        IF v_current_record.staff_id IS NULL THEN
            INSERT INTO dim_staff (
                staff_key,
                first_name,
                last_name,
                position,
                department,
                hire_date,
                is_active,
                start_date,
                version_number
            )
            VALUES (
                NEW.staff_email,
                NEW.first_name,
                NEW.last_name,
                NEW.position,
                v_department,
                NEW.hire_date,
                TRUE,
                CURRENT_TIMESTAMP,
                1
            )
            RETURNING staff_id INTO v_staff_id;
        ELSE
            -- Check if any relevant attributes changed
            IF EXISTS (
                SELECT 1
                FROM dim_staff
                WHERE staff_id = v_current_record.staff_id
                AND (
                    first_name != NEW.first_name
                    OR last_name != NEW.last_name
                    OR position != NEW.position
                )
            ) THEN
                -- Close current record
                UPDATE dim_staff
                SET is_current = FALSE,
                    end_date = CURRENT_TIMESTAMP
                WHERE staff_id = v_current_record.staff_id;
                
                -- Create new record
                INSERT INTO dim_staff (
                    staff_key,
                    first_name,
                    last_name,
                    position,
                    department,
                    hire_date,
                    is_active,
                    start_date,
                    version_number
                )
                VALUES (
                    NEW.staff_email,
                    NEW.first_name,
                    NEW.last_name,
                    NEW.position,
                    v_department,
                    NEW.hire_date,
                    TRUE,
                    CURRENT_TIMESTAMP,
                    (SELECT version_number + 1 FROM dim_staff WHERE staff_id = v_current_record.staff_id)
                )
                RETURNING staff_id INTO v_staff_id;
            ELSE
                v_staff_id := v_current_record.staff_id;
            END IF;
        END IF;
        
        -- Log success
        PERFORM log_etl_operation('update_staff_dimension', 'COMPLETED', 1);
        
        RETURN NEW;
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'update_staff_dimension',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql; 