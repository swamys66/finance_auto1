-- ============================================================================
-- Script to Check if Headers are Exported in S3 Stage File
-- ============================================================================
-- This script helps verify that CSV headers are present in the exported file
--
-- Usage:
--   1. Update the FILE_NAME variable to match your exported file
--   2. Run the queries below to check header presence
-- ============================================================================

-- ============================================================================
-- STEP 1: List files in the stage to find the latest export
-- ============================================================================
LIST @dev_data_ingress.finance.s3_test_finance_automation_output 
PATTERN = 'partner_finance_mapped%'
ORDER BY LAST_MODIFIED DESC;

-- ============================================================================
-- STEP 2: Check the first row(s) of the exported file to see if headers exist
-- ============================================================================
-- Replace 'partner_finance_mapped_YYYYMM.csv' with your actual filename
SET FILE_NAME = 'partner_finance_mapped_202509.csv';  -- Update with your file name

-- Query first 3 rows to check for headers
SELECT 
    $1 AS first_column,
    $2 AS second_column,
    $3 AS third_column,
    $4 AS fourth_column,
    $5 AS fifth_column,
    METADATA$FILE_ROW_NUMBER AS row_number,
    METADATA$FILENAME AS file_name
FROM @dev_data_ingress.finance.s3_test_finance_automation_output/$FILE_NAME
WHERE METADATA$FILE_ROW_NUMBER <= 3
ORDER BY METADATA$FILE_ROW_NUMBER;

-- ============================================================================
-- STEP 3: Check if first row contains expected header values
-- ============================================================================
-- This query checks if the first row matches expected header column names
-- Update the expected headers based on your actual column names
SELECT 
    CASE 
        WHEN $1 = 'ORACLE_CUSTOMER_NAME' 
         AND $2 = 'ORACLE_CUSTOMER_NAME_ID'
         AND $3 = 'ORACLE_INVOICE_GROUP'
        THEN 'HEADERS FOUND - First row contains expected header values'
        WHEN $1 LIKE '%.%' OR $1 LIKE '%_%' OR LENGTH($1) > 20
        THEN 'HEADERS LIKELY PRESENT - First row contains text values (likely headers)'
        WHEN TRY_TO_NUMBER($1) IS NOT NULL
        THEN 'NO HEADERS - First row contains numeric data (likely data row)'
        ELSE 'UNCERTAIN - First row format unclear'
    END AS header_check_status,
    $1 AS first_column_sample,
    $2 AS second_column_sample,
    $3 AS third_column_sample,
    METADATA$FILE_ROW_NUMBER AS row_number
FROM @dev_data_ingress.finance.s3_test_finance_automation_output/$FILE_NAME
WHERE METADATA$FILE_ROW_NUMBER = 1;

-- ============================================================================
-- STEP 4: Compare row count - if headers exist, row count should be data_rows + 1
-- ============================================================================
-- Get total row count from file
WITH file_stats AS (
    SELECT 
        MAX(METADATA$FILE_ROW_NUMBER) AS total_rows_in_file,
        COUNT(*) AS total_records
    FROM @dev_data_ingress.finance.s3_test_finance_automation_output/$FILE_NAME
),
source_stats AS (
    SELECT COUNT(*) AS source_row_count
    FROM dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping
)
SELECT 
    'FILE STATISTICS' AS check_type,
    fs.total_rows_in_file AS file_row_count,
    ss.source_row_count AS source_row_count,
    fs.total_rows_in_file - ss.source_row_count AS difference,
    CASE 
        WHEN fs.total_rows_in_file = ss.source_row_count + 1 
        THEN 'HEADERS PRESENT - File has 1 extra row (header row)'
        WHEN fs.total_rows_in_file = ss.source_row_count 
        THEN 'NO HEADERS - File row count matches source (no header row)'
        ELSE 'ROW COUNT MISMATCH - Check manually'
    END AS header_analysis
FROM file_stats fs
CROSS JOIN source_stats ss;

-- ============================================================================
-- STEP 5: Detailed header validation - Check first row against known headers
-- ============================================================================
-- This query shows the first row and compares it to expected headers
WITH first_row AS (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
        $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
        $41, $42, $43
    FROM @dev_data_ingress.finance.s3_test_finance_automation_output/$FILE_NAME
    WHERE METADATA$FILE_ROW_NUMBER = 1
),
expected_headers AS (
    SELECT 
        'ORACLE_CUSTOMER_NAME' AS col1,
        'ORACLE_CUSTOMER_NAME_ID' AS col2,
        'ORACLE_INVOICE_GROUP' AS col3,
        'ORACLE_INVOICE_NAME' AS col4,
        'ORACLE_GL_ACCOUNT' AS col5
)
SELECT 
    'HEADER VALIDATION' AS check_type,
    CASE 
        WHEN fr.$1 = eh.col1 AND fr.$2 = eh.col2 AND fr.$3 = eh.col3
        THEN '✓ HEADERS CONFIRMED - First row matches expected header pattern'
        ELSE '✗ HEADERS NOT FOUND - First row does not match expected headers'
    END AS validation_result,
    fr.$1 AS first_column,
    fr.$2 AS second_column,
    fr.$3 AS third_column,
    fr.$4 AS fourth_column,
    fr.$5 AS fifth_column
FROM first_row fr
CROSS JOIN expected_headers eh;

-- ============================================================================
-- STEP 6: Quick visual check - Show first 5 rows side by side
-- ============================================================================
SELECT 
    METADATA$FILE_ROW_NUMBER AS row_num,
    CASE 
        WHEN METADATA$FILE_ROW_NUMBER = 1 THEN 'HEADER ROW (if headers exist)'
        ELSE 'DATA ROW'
    END AS row_type,
    $1 AS col1,
    $2 AS col2,
    $3 AS col3,
    SUBSTRING($1 || ',' || $2 || ',' || $3 || '...', 1, 100) AS first_three_cols_preview
FROM @dev_data_ingress.finance.s3_test_finance_automation_output/$FILE_NAME
WHERE METADATA$FILE_ROW_NUMBER <= 5
ORDER BY METADATA$FILE_ROW_NUMBER;

