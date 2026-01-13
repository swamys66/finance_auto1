-- Snowflake Tasks for Finance Revenue Mapping Automation
-- This script creates Snowflake tasks to automate the import and processing

-- ============================================================================
-- PREREQUISITES
-- ============================================================================
-- 1. Ensure you have TASKADMIN role or appropriate privileges
-- 2. Create a warehouse for task execution (or use existing)
-- 3. Tasks will run on a schedule defined below

-- ============================================================================
-- STEP 1: Create Warehouse for Tasks (if needed)
-- ============================================================================
CREATE WAREHOUSE IF NOT EXISTS TASK_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- ============================================================================
-- STEP 2: Create Task 1: Import CSV from S3
-- ============================================================================
-- This task imports the CSV mapping file from S3
-- Schedule: Daily at 2 AM UTC (adjust as needed)

CREATE OR REPLACE TASK import_csv_from_s3
    WAREHOUSE = TASK_WH
    SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- Daily at 2 AM UTC
    COMMENT = 'Import CSV mapping file from S3 bucket'
AS
BEGIN
    -- Create external stage (if not exists)
    CREATE STAGE IF NOT EXISTS dev_data_ingress.finance.s3_mapping_import
        URL = 's3://your-bucket-name/mapping-files/'
        CREDENTIALS = (
            AWS_KEY_ID = 'your-aws-access-key-id'
            AWS_SECRET_KEY = 'your-aws-secret-access-key'
        )
        FILE_FORMAT = (TYPE = 'CSV' 
                       SKIP_HEADER = 1 
                       FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                       ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE);

    -- Create table structure
    CREATE TABLE IF NOT EXISTS dev_data_ingress.finance.mapping_template_raw_CURSOR (
        ID VARCHAR,
        Oracle_Customer_Name VARCHAR,
        Oracle_Customer_Name_ID VARCHAR,
        Oracle_Invoice_Group VARCHAR,
        Oracle_Invoice_Name VARCHAR,
        Oracle_GL_Account VARCHAR
    );

    -- Truncate existing data
    TRUNCATE TABLE dev_data_ingress.finance.mapping_template_raw_CURSOR;

    -- Load from S3 (adjust file name/pattern as needed)
    COPY INTO dev_data_ingress.finance.mapping_template_raw_CURSOR
    FROM @dev_data_ingress.finance.s3_mapping_import/
    FILE_FORMAT = (TYPE = 'CSV' 
                   SKIP_HEADER = 1 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                   ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
    PATTERN = '.*mapping.*\\.csv'
    ON_ERROR = 'ABORT_STATEMENT';
END;

-- ============================================================================
-- STEP 3: Create Task 2: Data Quality Checks (Raw)
-- ============================================================================
-- This task runs after import completes
-- Note: Tasks can only call stored procedures or SQL statements, not full scripts
-- For complex logic, create a stored procedure

CREATE OR REPLACE TASK data_quality_checks_raw
    WAREHOUSE = TASK_WH
    AFTER import_csv_from_s3
    COMMENT = 'Run data quality checks on raw mapping data'
AS
BEGIN
    -- Quick validation check
    DECLARE
        row_count INTEGER;
        unique_count INTEGER;
    BEGIN
        SELECT COUNT(*), COUNT(DISTINCT ID)
        INTO row_count, unique_count
        FROM dev_data_ingress.finance.mapping_template_raw_CURSOR;
        
        IF row_count = 0 THEN
            RAISE EXCEPTION 'Data quality check failed: No rows loaded';
        END IF;
        
        IF row_count != unique_count THEN
            RAISE EXCEPTION 'Data quality check failed: Duplicate IDs found';
        END IF;
    END;
END;

-- ============================================================================
-- STEP 4: Create Task 3: Create Merged View
-- ============================================================================
CREATE OR REPLACE TASK create_mapped_view
    WAREHOUSE = TASK_WH
    AFTER data_quality_checks_raw
    COMMENT = 'Create merged view with revenue aggregation'
AS
BEGIN
    CREATE OR REPLACE VIEW dev_data_ingress.finance.view_partner_finance_mapped AS
    SELECT 
        r.*,
        m.Oracle_Customer_Name,
        m.Oracle_Customer_Name_ID,
        m.Oracle_Invoice_Group,
        m.Oracle_Invoice_Name,
        m.Oracle_GL_Account
    FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    LEFT JOIN dev_data_ingress.finance.mapping_template_raw_CURSOR m
        ON r.ID = m.ID
    WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()));
END;

-- ============================================================================
-- STEP 5: Create Task 4: Export to S3
-- ============================================================================
CREATE OR REPLACE TASK export_to_s3
    WAREHOUSE = TASK_WH
    AFTER create_mapped_view
    COMMENT = 'Export merged view to S3 bucket'
AS
BEGIN
    -- Create export stage (if not exists)
    CREATE STAGE IF NOT EXISTS dev_data_ingress.finance.s3_finance_export
        URL = 's3://your-bucket-name/finance-revenue-exports/'
        CREDENTIALS = (
            AWS_KEY_ID = 'your-aws-access-key-id'
            AWS_SECRET_KEY = 'your-aws-secret-access-key'
        )
        FILE_FORMAT = (TYPE = 'CSV' 
                       COMPRESSION = 'GZIP' 
                       FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                       HEADER = TRUE);

    -- Export to S3 with timestamp
    COPY INTO @dev_data_ingress.finance.s3_finance_export/partner_finance_mapped_
        || TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'YYYYMM') 
        || '.csv.gz'
    FROM (
        SELECT * 
        FROM dev_data_ingress.finance.view_partner_finance_mapped
        ORDER BY ID
    )
    FILE_FORMAT = (TYPE = 'CSV' 
                   COMPRESSION = 'GZIP' 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                   HEADER = TRUE)
    SINGLE = TRUE
    OVERWRITE = TRUE;
END;

-- ============================================================================
-- STEP 6: Resume Tasks (Tasks are created in SUSPENDED state)
-- ============================================================================
ALTER TASK import_csv_from_s3 RESUME;
ALTER TASK data_quality_checks_raw RESUME;
ALTER TASK create_mapped_view RESUME;
ALTER TASK export_to_s3 RESUME;

-- ============================================================================
-- STEP 7: Verify Task Status
-- ============================================================================
-- Check task status
SHOW TASKS LIKE '%finance%';

-- Check task execution history
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE '%finance%'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- ============================================================================
-- STEP 8: Manual Task Execution (for testing)
-- ============================================================================
-- Execute a task manually for testing
-- EXECUTE TASK import_csv_from_s3;

-- ============================================================================
-- STEP 9: Suspend/Resume Tasks
-- ============================================================================
-- Suspend all tasks
-- ALTER TASK import_csv_from_s3 SUSPEND;
-- ALTER TASK data_quality_checks_raw SUSPEND;
-- ALTER TASK create_mapped_view SUSPEND;
-- ALTER TASK export_to_s3 SUSPEND;

-- Resume all tasks
-- ALTER TASK import_csv_from_s3 RESUME;
-- ALTER TASK data_quality_checks_raw RESUME;
-- ALTER TASK create_mapped_view RESUME;
-- ALTER TASK export_to_s3 RESUME;

-- ============================================================================
-- STEP 10: Drop Tasks (if needed)
-- ============================================================================
-- Drop tasks in reverse order (due to dependencies)
-- DROP TASK IF EXISTS export_to_s3;
-- DROP TASK IF EXISTS create_mapped_view;
-- DROP TASK IF EXISTS data_quality_checks_raw;
-- DROP TASK IF EXISTS import_csv_from_s3;

