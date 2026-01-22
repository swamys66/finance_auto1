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
-- STEP 1: Extract data_month from source table dynamically
-- ============================================================================
-- Get the data_month value and format as YYYYMM
SET (MONTH_STR) = (
    SELECT DISTINCT TO_CHAR(data_month, 'YYYYMM') AS month_str
    FROM dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping
    WHERE data_month IS NOT NULL
    ORDER BY month_str DESC
    LIMIT 1
);

-- Construct filename
SET FILE_NAME = 'partner_finance_mapped_' || $MONTH_STR || '.csv';

-- Display the filename that will be used
SELECT 
    'File will be exported as: ' || $FILE_NAME AS export_info,
    'Data month: ' || $MONTH_STR AS data_month_info;

-- ============================================================================
-- STEP 2: Remove existing file (if overwrite is needed)
-- ============================================================================
-- Uncomment the line below if you want to remove existing file first
-- REMOVE @dev_data_ingress.finance.s3_test_finance_automation_output/$FILE_NAME;

-- ============================================================================
-- STEP 3: Export to S3
-- ============================================================================
COPY INTO @dev_data_ingress.finance.s3_test_finance_automation_output/$FILE_NAME
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

-- ============================================================================
-- STEP 4: Verify Export
-- ============================================================================
SELECT 
    'EXPORT COMPLETED' AS status,
    $FILE_NAME AS exported_file,
    'dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping' AS source_table,
    $MONTH_STR AS data_month,
    CURRENT_TIMESTAMP() AS export_timestamp;

-- ============================================================================
-- STEP 5: List exported file (optional verification)
-- ============================================================================
LIST @dev_data_ingress.finance.s3_test_finance_automation_output PATTERN = $FILE_NAME;

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

