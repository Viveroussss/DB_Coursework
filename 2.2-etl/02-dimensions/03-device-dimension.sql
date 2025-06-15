-- =====================
-- 📱 Device Dimension ETL
-- =====================

-- Function to handle device dimension with improved error handling
CREATE OR REPLACE FUNCTION update_device_dimension()
RETURNS TRIGGER AS $$
DECLARE
    v_device_id INTEGER;
    v_device_type VARCHAR(50);
    v_is_mobile BOOLEAN;
    v_is_tablet BOOLEAN;
    v_is_desktop BOOLEAN;
    v_log_id INTEGER;
BEGIN
    -- Start logging
    v_log_id := log_etl_operation('update_device_dimension', 'STARTED');
    
    BEGIN
        -- Determine device type and characteristics
        v_device_type := CASE 
            WHEN NEW.device ILIKE '%Mobile%' THEN 'Mobile'
            WHEN NEW.device ILIKE '%Tablet%' THEN 'Tablet'
            ELSE 'Desktop'
        END;
        
        v_is_mobile := v_device_type = 'Mobile';
        v_is_tablet := v_device_type = 'Tablet';
        v_is_desktop := v_device_type = 'Desktop';
        
        -- Check if device exists
        SELECT device_id INTO v_device_id
        FROM dim_device
        WHERE device_type = v_device_type
        AND device_model = NEW.device;
        
        -- If device doesn't exist, create new record
        IF v_device_id IS NULL THEN
            INSERT INTO dim_device (
                device_type,
                device_model,
                is_mobile,
                is_tablet,
                is_desktop
            )
            VALUES (
                v_device_type,
                NEW.device,
                v_is_mobile,
                v_is_tablet,
                v_is_desktop
            )
            RETURNING device_id INTO v_device_id;
        END IF;
        
        -- Log success
        PERFORM log_etl_operation('update_device_dimension', 'COMPLETED', 1);
        
        RETURN NEW;
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            PERFORM log_etl_operation(
                'update_device_dimension',
                'ERROR',
                0,
                SQLERRM
            );
            RAISE;
    END;
END;
$$ LANGUAGE plpgsql; 