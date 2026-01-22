-- Investigate why ID '00.248.000.90002.000.000000' is being dropped from export
-- Run these queries to diagnose the issue

-- ============================================================================
-- STEP 1: Check if the record exists in source table
-- ============================================================================
SELECT 
    'RECORD EXISTS CHECK' AS check_type,
    COUNT(*) AS record_count
FROM dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping
WHERE ID = '00.248.000.90002.000.000000';

-- ============================================================================
-- STEP 2: View the actual record data
-- ============================================================================
SELECT *
FROM dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping
WHERE ID = '00.248.000.90002.000.000000';

-- ============================================================================
-- STEP 3: Check for problematic data in this specific record
-- ============================================================================
SELECT 
    ID,
    -- Check each column for potential CSV issues
    CASE WHEN Oracle_Customer_Name IS NULL THEN 'NULL' ELSE 'HAS_VALUE' END AS Oracle_Customer_Name_status,
    CASE WHEN Oracle_Customer_Name_ID IS NULL THEN 'NULL' ELSE 'HAS_VALUE' END AS Oracle_Customer_Name_ID_status,
    CASE WHEN Oracle_Invoice_Group IS NULL THEN 'NULL' ELSE 'HAS_VALUE' END AS Oracle_Invoice_Group_status,
    CASE WHEN Oracle_Invoice_Name IS NULL THEN 'NULL' ELSE 'HAS_VALUE' END AS Oracle_Invoice_Name_status,
    CASE WHEN Oracle_GL_Account IS NULL THEN 'NULL' ELSE 'HAS_VALUE' END AS Oracle_GL_Account_status,
    -- Check for special characters that might cause CSV parsing issues
    CASE WHEN REGEXP_LIKE(COALESCE(Oracle_Customer_Name, ''), '\n|\r|"') THEN 'HAS_SPECIAL_CHARS' ELSE 'OK' END AS Oracle_Customer_Name_chars,
    CASE WHEN REGEXP_LIKE(COALESCE(Oracle_Invoice_Name, ''), '\n|\r|"') THEN 'HAS_SPECIAL_CHARS' ELSE 'OK' END AS Oracle_Invoice_Name_chars,
    -- Check if ID itself has issues
    LENGTH(ID) AS id_length,
    CASE WHEN ID LIKE '%.%' THEN 'HAS_DOTS' ELSE 'NO_DOTS' END AS id_format
FROM dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping
WHERE ID = '00.248.000.90002.000.000000';

-- ============================================================================
-- STEP 4: Check if this ID appears in exported file
-- ============================================================================
SELECT 
    'EXPORTED FILE CHECK' AS check_type,
    COUNT(*) AS exported_count
FROM @dev_data_ingress.finance.s3_test_finance_automation_output
WHERE METADATA$FILENAME = 'temp_check'
AND $1 = '00.248.000.90002.000.000000';  -- Assuming ID is first column

-- ============================================================================
-- STEP 5: Test export with this specific record only
-- ============================================================================
-- Export just this one record to see if it works in isolation
COPY INTO @dev_data_ingress.finance.s3_test_finance_automation_output/test_single_record.csv
FROM (
    SELECT * 
    FROM dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping
    WHERE ID = '00.248.000.90002.000.000000'
    ORDER BY ID
)
FILE_FORMAT = (TYPE = 'CSV' 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
               NULL_IF = ('NULL', 'null', '')
               ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
SINGLE = TRUE
OVERWRITE = TRUE
ON_ERROR = 'CONTINUE';

-- Check if the single record export worked
SELECT 
    'SINGLE RECORD EXPORT CHECK' AS check_type,
    MAX(METADATA$FILE_ROW_NUMBER) AS exported_rows
FROM @dev_data_ingress.finance.s3_test_finance_automation_output
WHERE METADATA$FILENAME = 'test_single_record.csv';

-- ============================================================================
-- STEP 6: Compare all IDs - source vs exported
-- ============================================================================
WITH source_ids AS (
    SELECT DISTINCT ID
    FROM dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping
    WHERE ID IS NOT NULL
),
exported_ids AS (
    SELECT DISTINCT $1 AS ID  -- Assuming ID is first column
    FROM @dev_data_ingress.finance.s3_test_finance_automation_output
    WHERE METADATA$FILENAME = 'temp_check'
    AND $1 IS NOT NULL
)
SELECT 
    'MISSING IN EXPORT' AS status,
    s.ID AS missing_id
FROM source_ids s
LEFT JOIN exported_ids e ON s.ID = e.ID
WHERE e.ID IS NULL
ORDER BY s.ID;

