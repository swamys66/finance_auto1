-- Snowflake SQL Script: Export to S3
-- Standalone SQL script for exporting data to S3 using COPY INTO
-- 
-- This script:
-- 1. Exports data from a table/view to S3 stage
-- 2. Supports dynamic timestamp in filename (like 6th program)
-- 3. No compression (for smaller datasets)
-- 4. Single file export
--
-- Usage:
--   1. Update the variables below with your values
--   2. Run this script in Snowflake
--
-- Parameters to update:
--   - STAGE_NAME: Full stage name (e.g., 'dev_data_ingress.finance.s3_test_finance_automation_output')
--   - FILE_PREFIX: File name prefix (e.g., 'partner_finance_mapped')
--   - SOURCE_TABLE: Full table/view name to export from (e.g., 'dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping')
--   - ORDER_BY_COLUMN: Column to order by (default: 'ID')
--   - DATA_MONTH_COLUMN: Column name containing the data_month value (default: 'data_month')
--   - OVERWRITE: Whether to overwrite existing files (default: true)

-- ============================================================================
-- STEP 1: Set Variables (Update these values)
-- ============================================================================
SET STAGE_NAME = 'dev_data_ingress.finance.s3_test_finance_automation_output';
SET FILE_PREFIX = 'partner_finance_mapped';
SET SOURCE_TABLE = 'dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping';
SET ORDER_BY_COLUMN = 'ID';
SET DATA_MONTH_COLUMN = 'data_month';
SET OVERWRITE = TRUE;

-- ============================================================================
-- STEP 2: Extract data_month from source table dynamically
-- ============================================================================
SET DATA_MONTH_QUERY = $$
    SELECT DISTINCT TO_CHAR($DATA_MONTH_COLUMN, 'YYYYMM') AS month_str
    FROM $SOURCE_TABLE
    WHERE $DATA_MONTH_COLUMN IS NOT NULL
    ORDER BY month_str DESC
    LIMIT 1
$$;

-- Replace variables in query
SET DATA_MONTH_QUERY = REPLACE(REPLACE(REPLACE($DATA_MONTH_QUERY, '$DATA_MONTH_COLUMN', $DATA_MONTH_COLUMN), '$SOURCE_TABLE', $SOURCE_TABLE), '$$', '');

-- Execute query to get month string
SET MONTH_STR = (SELECT month_str FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

-- Construct filename
SET FILE_NAME = $FILE_PREFIX || '_' || $MONTH_STR || '.csv';

-- ============================================================================
-- STEP 3: Remove existing file if overwrite is true
-- ============================================================================
BEGIN
    IF ($OVERWRITE) THEN
        BEGIN
            REMOVE @$STAGE_NAME/$FILE_NAME;
            SELECT 'Removed existing file: ' || $FILE_NAME AS removal_status;
        EXCEPTION
            WHEN OTHER THEN
                SELECT 'File does not exist or already removed: ' || $FILE_NAME AS removal_status;
        END;
    END IF;
END;

-- ============================================================================
-- STEP 4: Export to S3
-- ============================================================================
COPY INTO @$STAGE_NAME/$FILE_NAME
FROM (
    SELECT * 
    FROM IDENTIFIER($SOURCE_TABLE)
    ORDER BY IDENTIFIER($ORDER_BY_COLUMN)
)
FILE_FORMAT = (TYPE = 'CSV' 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
               NULL_IF = ('NULL', 'null', ''))
SINGLE = TRUE
OVERWRITE = TRUE;

-- ============================================================================
-- STEP 5: Verify Export
-- ============================================================================
SELECT 
    'EXPORT COMPLETED' AS status,
    $FILE_NAME AS exported_file,
    $SOURCE_TABLE AS source_table,
    $MONTH_STR AS data_month,
    CURRENT_TIMESTAMP() AS export_timestamp;

-- ============================================================================
-- STEP 6: List exported file (optional verification)
-- ============================================================================
LIST @$STAGE_NAME PATTERN = $FILE_NAME;

-- ============================================================================
-- ALTERNATIVE VERSION: Using direct SQL without variables (simpler)
-- ============================================================================
/*
-- Direct version - update values inline:

-- Step 1: Get data_month value
SET MONTH_STR = (
    SELECT DISTINCT TO_CHAR(data_month, 'YYYYMM') AS month_str
    FROM dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping
    WHERE data_month IS NOT NULL
    ORDER BY month_str DESC
    LIMIT 1
);

-- Step 2: Remove existing file (if needed)
REMOVE @dev_data_ingress.finance.s3_test_finance_automation_output/partner_finance_mapped_202512.csv;

-- Step 3: Export
COPY INTO @dev_data_ingress.finance.s3_test_finance_automation_output/partner_finance_mapped_202512.csv
FROM (
    SELECT * 
    FROM dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping
    ORDER BY ID
)
FILE_FORMAT = (TYPE = 'CSV' 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
               NULL_IF = ('NULL', 'null', ''))
SINGLE = TRUE
OVERWRITE = TRUE;
*/

