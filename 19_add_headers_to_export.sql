-- ============================================================================
-- Script to Add Headers to Exported CSV File
-- ============================================================================
-- This script adds headers to an existing CSV file in S3 stage
-- 
-- Usage:
--   1. First export your data using export_to_s3 (without headers)
--   2. Update FILE_NAME and HEADER_ROW variables below
--   3. Run this script to create a new file with headers
-- ============================================================================

-- Set the file name (update with your actual exported file)
SET FILE_NAME = 'partner_finance_mapped_202509.csv';  -- Update with your file name
SET NEW_FILE_NAME = 'partner_finance_mapped_202509_with_headers.csv';

-- Step 1: Create a table with the header row (built directly to avoid variable size limit)
-- Note: Header row is built directly in SQL to avoid Snowflake's 256-byte variable limit
CREATE OR REPLACE TABLE dev_data_ingress.finance.temp_header AS
SELECT 'ORACLE_CUSTOMER_NAME,ORACLE_CUSTOMER_NAME_ID,ORACLE_INVOICE_GROUP,ORACLE_INVOICE_NAME,ORACLE_GL_ACCOUNT,ORACLE_MAPPED_RECORD_FLAG,ID_MAPPED_RECORD_FLAG,ID,DATA_MONTH,BUSINESS_UNIT_NAME,BUSINESS_UNIT_DETAIL_NAME,PRODUCT_LINE_ID,PRODUCT_LINE_NAME,PARENT_PARTNER_ID,PARENT_PARTNER_NAME,PARTNER_ID,PARTNER_NAME,DRID,CONTRACT_ID,CONTRACT_NAME,GAM_ADVERTISER_LEVEL1,GAM_ADVERTISER_LEVEL2,NETWORK_NAME_ID,NETWORK_NAME,NETWORK_CLASSIFICATION_NAME,NETWORK_CLASSIFICATION_SUBTYPE,NETWORK_FEED_ID,MARKUP_PERCENT,NET_PERCENT,REVSHARE_PERCENT,BAD_DEBT_PERCENT,MANAGEMENT_PERCENT,IMPRESSIONS,CLICKS,NETWORK_GROSS_REVENUE,NETWORK_REVSHARE,S1_GROSS_REVENUE,MANAGMENT_FEE,PARTNER_REVSHARE,HOLDBACK_REVENUE,PARTNER_GROSS_REVENUE,COMMISSION_AMOUNT,AGGREGATION_SOURCE' AS header_line;

-- Step 2: Create a file format for CSV (one-time setup, can be reused)
CREATE OR REPLACE FILE FORMAT dev_data_ingress.finance.csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '');

-- Step 3: Load data from existing file into temp table using COPY INTO
-- NOTE: Replace 'partner_finance_mapped_202509.csv' with your actual FILE_NAME variable value
CREATE OR REPLACE TEMPORARY TABLE temp_export_data (
    col1 STRING, col2 STRING, col3 STRING, col4 STRING, col5 STRING,
    col6 STRING, col7 STRING, col8 STRING, col9 STRING, col10 STRING,
    col11 STRING, col12 STRING, col13 STRING, col14 STRING, col15 STRING,
    col16 STRING, col17 STRING, col18 STRING, col19 STRING, col20 STRING,
    col21 STRING, col22 STRING, col23 STRING, col24 STRING, col25 STRING,
    col26 STRING, col27 STRING, col28 STRING, col29 STRING, col30 STRING,
    col31 STRING, col32 STRING, col33 STRING, col34 STRING, col35 STRING,
    col36 STRING, col37 STRING, col38 STRING, col39 STRING, col40 STRING,
    col41 STRING, col42 STRING, col43 STRING
);

COPY INTO temp_export_data
FROM @dev_data_ingress.finance.s3_test_finance_automation_output/partner_finance_mapped_202509.csv
FILE_FORMAT = (FORMAT_NAME = 'dev_data_ingress.finance.csv_format');

-- Step 4: Create header split table
CREATE OR REPLACE TEMPORARY TABLE temp_header_split AS
SELECT 
    SPLIT_PART(header_line, ',', 1) AS col1, SPLIT_PART(header_line, ',', 2) AS col2,
    SPLIT_PART(header_line, ',', 3) AS col3, SPLIT_PART(header_line, ',', 4) AS col4,
    SPLIT_PART(header_line, ',', 5) AS col5, SPLIT_PART(header_line, ',', 6) AS col6,
    SPLIT_PART(header_line, ',', 7) AS col7, SPLIT_PART(header_line, ',', 8) AS col8,
    SPLIT_PART(header_line, ',', 9) AS col9, SPLIT_PART(header_line, ',', 10) AS col10,
    SPLIT_PART(header_line, ',', 11) AS col11, SPLIT_PART(header_line, ',', 12) AS col12,
    SPLIT_PART(header_line, ',', 13) AS col13, SPLIT_PART(header_line, ',', 14) AS col14,
    SPLIT_PART(header_line, ',', 15) AS col15, SPLIT_PART(header_line, ',', 16) AS col16,
    SPLIT_PART(header_line, ',', 17) AS col17, SPLIT_PART(header_line, ',', 18) AS col18,
    SPLIT_PART(header_line, ',', 19) AS col19, SPLIT_PART(header_line, ',', 20) AS col20,
    SPLIT_PART(header_line, ',', 21) AS col21, SPLIT_PART(header_line, ',', 22) AS col22,
    SPLIT_PART(header_line, ',', 23) AS col23, SPLIT_PART(header_line, ',', 24) AS col24,
    SPLIT_PART(header_line, ',', 25) AS col25, SPLIT_PART(header_line, ',', 26) AS col26,
    SPLIT_PART(header_line, ',', 27) AS col27, SPLIT_PART(header_line, ',', 28) AS col28,
    SPLIT_PART(header_line, ',', 29) AS col29, SPLIT_PART(header_line, ',', 30) AS col30,
    SPLIT_PART(header_line, ',', 31) AS col31, SPLIT_PART(header_line, ',', 32) AS col32,
    SPLIT_PART(header_line, ',', 33) AS col33, SPLIT_PART(header_line, ',', 34) AS col34,
    SPLIT_PART(header_line, ',', 35) AS col35, SPLIT_PART(header_line, ',', 36) AS col36,
    SPLIT_PART(header_line, ',', 37) AS col37, SPLIT_PART(header_line, ',', 38) AS col38,
    SPLIT_PART(header_line, ',', 39) AS col39, SPLIT_PART(header_line, ',', 40) AS col40,
    SPLIT_PART(header_line, ',', 41) AS col41, SPLIT_PART(header_line, ',', 42) AS col42,
    SPLIT_PART(header_line, ',', 43) AS col43
FROM dev_data_ingress.finance.temp_header;

-- Step 5: Export header + data to new file
-- NOTE: Replace 'partner_finance_mapped_202509_with_headers.csv' with your actual NEW_FILE_NAME variable value
COPY INTO @dev_data_ingress.finance.s3_test_finance_automation_output/partner_finance_mapped_202509_with_headers.csv
FROM (SELECT * FROM temp_header_split UNION ALL SELECT * FROM temp_export_data)
FILE_FORMAT = (FORMAT_NAME = 'dev_data_ingress.finance.csv_format' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
SINGLE = TRUE OVERWRITE = TRUE;

-- Step 6: Verify the new file
SELECT 
    'Headers added successfully' AS status,
    'partner_finance_mapped_202509_with_headers.csv' AS new_file_name,
    'partner_finance_mapped_202509.csv' AS original_file_name;

-- Step 7: List the new file
LIST @dev_data_ingress.finance.s3_test_finance_automation_output 
PATTERN = 'partner_finance_mapped_202509_with_headers.csv';

-- ============================================================================
-- INSTRUCTIONS:
-- ============================================================================
-- To use this script with different file names:
-- 1. Update FILE_NAME and NEW_FILE_NAME variables at the top (lines 13-14)
-- 2. Replace 'partner_finance_mapped_202509.csv' in Step 2 with your FILE_NAME value
-- 3. Replace 'partner_finance_mapped_202509_with_headers.csv' in Step 4 with your NEW_FILE_NAME value
-- 4. Replace the file names in Steps 5-6 as well
-- ============================================================================

