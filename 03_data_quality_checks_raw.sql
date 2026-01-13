-- Data Quality Checks for Raw Mapping Data
-- Table: dev_data_ingress.finance.mapping_template_raw_CURSOR

-- ============================================================================
-- 1. UNIQUENESS CHECK: Verify ID is unique
-- ============================================================================
SELECT 
    'Uniqueness Check' AS check_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT ID) AS unique_ids,
    COUNT(*) - COUNT(DISTINCT ID) AS duplicate_count,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT ID) THEN 'PASS'
        ELSE 'FAIL - Duplicate IDs found'
    END AS status
FROM dev_data_ingress.finance.mapping_template_raw_CURSOR;

-- Show duplicate IDs if any
SELECT 
    ID,
    COUNT(*) AS occurrence_count
FROM dev_data_ingress.finance.mapping_template_raw_CURSOR
GROUP BY ID
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;

-- ============================================================================
-- 2. COMPLETENESS CHECK: Check for NULL values in critical fields
-- ============================================================================
SELECT 
    'Completeness Check' AS check_name,
    COUNT(*) AS total_records,
    COUNT(ID) AS non_null_ids,
    COUNT(Oracle_Customer_Name) AS non_null_customer_names,
    COUNT(Oracle_Customer_Name_ID) AS non_null_customer_name_ids,
    COUNT(Oracle_Invoice_Group) AS non_null_invoice_groups,
    COUNT(Oracle_Invoice_Name) AS non_null_invoice_names,
    COUNT(Oracle_GL_Account) AS non_null_gl_accounts,
    COUNT(*) - COUNT(ID) AS null_ids,
    COUNT(*) - COUNT(Oracle_Customer_Name) AS null_customer_names,
    COUNT(*) - COUNT(Oracle_GL_Account) AS null_gl_accounts
FROM dev_data_ingress.finance.mapping_template_raw_CURSOR;

-- Records with NULL IDs (should be zero)
SELECT *
FROM dev_data_ingress.finance.mapping_template_raw_CURSOR
WHERE ID IS NULL;

-- Records with NULL in critical fields
SELECT 
    ID,
    CASE WHEN Oracle_Customer_Name IS NULL THEN 'NULL' ELSE 'OK' END AS customer_name_status,
    CASE WHEN Oracle_GL_Account IS NULL THEN 'NULL' ELSE 'OK' END AS gl_account_status
FROM dev_data_ingress.finance.mapping_template_raw_CURSOR
WHERE Oracle_Customer_Name IS NULL 
   OR Oracle_GL_Account IS NULL;

-- ============================================================================
-- 3. DATA TYPE VALIDATION: Check for unexpected data patterns
-- ============================================================================
-- Check ID format (assuming it should be alphanumeric)
SELECT 
    'ID Format Check' AS check_name,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN ID REGEXP '^[A-Za-z0-9]+$' THEN 1 END) AS valid_format_ids,
    COUNT(*) - COUNT(CASE WHEN ID REGEXP '^[A-Za-z0-9]+$' THEN 1 END) AS invalid_format_ids
FROM dev_data_ingress.finance.mapping_template_raw_CURSOR;

-- Check for empty strings (treated as NULL)
SELECT 
    'Empty String Check' AS check_name,
    COUNT(CASE WHEN TRIM(ID) = '' THEN 1 END) AS empty_ids,
    COUNT(CASE WHEN TRIM(Oracle_Customer_Name) = '' THEN 1 END) AS empty_customer_names,
    COUNT(CASE WHEN TRIM(Oracle_GL_Account) = '' THEN 1 END) AS empty_gl_accounts
FROM dev_data_ingress.finance.mapping_template_raw_CURSOR;

-- ============================================================================
-- 4. DATA LENGTH VALIDATION: Check for unusually long or short values
-- ============================================================================
SELECT 
    'Data Length Check' AS check_name,
    MIN(LENGTH(ID)) AS min_id_length,
    MAX(LENGTH(ID)) AS max_id_length,
    AVG(LENGTH(ID)) AS avg_id_length,
    MIN(LENGTH(Oracle_Customer_Name)) AS min_customer_name_length,
    MAX(LENGTH(Oracle_Customer_Name)) AS max_customer_name_length,
    MIN(LENGTH(Oracle_GL_Account)) AS min_gl_account_length,
    MAX(LENGTH(Oracle_GL_Account)) AS max_gl_account_length
FROM dev_data_ingress.finance.mapping_template_raw_CURSOR;

-- ============================================================================
-- 5. COMPREHENSIVE DATA QUALITY SUMMARY
-- ============================================================================
WITH quality_checks AS (
    SELECT 
        COUNT(*) AS total_records,
        COUNT(DISTINCT ID) AS unique_ids,
        COUNT(CASE WHEN ID IS NULL THEN 1 END) AS null_ids,
        COUNT(CASE WHEN TRIM(ID) = '' THEN 1 END) AS empty_ids,
        COUNT(CASE WHEN Oracle_Customer_Name IS NULL THEN 1 END) AS null_customer_names,
        COUNT(CASE WHEN Oracle_GL_Account IS NULL THEN 1 END) AS null_gl_accounts,
        COUNT(CASE WHEN Oracle_Customer_Name_ID IS NULL THEN 1 END) AS null_customer_name_ids,
        COUNT(CASE WHEN Oracle_Invoice_Group IS NULL THEN 1 END) AS null_invoice_groups,
        COUNT(CASE WHEN Oracle_Invoice_Name IS NULL THEN 1 END) AS null_invoice_names
    FROM dev_data_ingress.finance.mapping_template_raw_CURSOR
)
SELECT 
    'DATA QUALITY SUMMARY' AS report_section,
    total_records,
    unique_ids,
    CASE 
        WHEN total_records = unique_ids THEN 'PASS'
        ELSE 'FAIL'
    END AS uniqueness_status,
    null_ids + empty_ids AS total_invalid_ids,
    CASE 
        WHEN null_ids + empty_ids = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS id_completeness_status,
    null_customer_names AS missing_customer_names,
    null_gl_accounts AS missing_gl_accounts,
    null_customer_name_ids AS missing_customer_name_ids,
    null_invoice_groups AS missing_invoice_groups,
    null_invoice_names AS missing_invoice_names,
    CASE 
        WHEN null_ids + empty_ids = 0 
         AND total_records = unique_ids 
         AND null_gl_accounts = 0 
        THEN 'PASS - Data quality acceptable'
        ELSE 'FAIL - Data quality issues detected'
    END AS overall_status
FROM quality_checks;

-- ============================================================================
-- 6. SAMPLE DATA PREVIEW
-- ============================================================================
SELECT 
    'Sample Data Preview' AS report_section,
    *
FROM dev_data_ingress.finance.mapping_template_raw_CURSOR
LIMIT 10;

