-- ============================================================================
-- Diagnostic Script to Check Compiled Export SQL
-- ============================================================================
-- Run this after dbt compile to see what SQL is actually being generated
-- ============================================================================

-- Check the compiled SQL file
-- Location: target/run/[project_name]/models/[path]/_3_export_to_s3.sql

-- Or run this to see the actual SQL being executed:
SELECT 
    'Check the compiled SQL file at: target/run/dev_data_ingress/models/dev_data_ingress/finance/_3_export_to_s3.sql' AS instruction,
    'Look for the COPY INTO statement and check if quotes are present around header values' AS note;

-- Test query to verify header select syntax works
-- Replace with your actual header values
SELECT 
    CAST('ORACLE_CUSTOMER_NAME' AS VARCHAR) AS col1,
    CAST('ORACLE_CUSTOMER_NAME_ID' AS VARCHAR) AS col2,
    CAST('ORACLE_INVOICE_GROUP' AS VARCHAR) AS col3
FROM (SELECT 1) AS t;

-- Test VALUES clause syntax
SELECT * FROM (VALUES (
    CAST('ORACLE_CUSTOMER_NAME' AS VARCHAR),
    CAST('ORACLE_CUSTOMER_NAME_ID' AS VARCHAR),
    CAST('ORACLE_INVOICE_GROUP' AS VARCHAR)
)) AS header_row;

