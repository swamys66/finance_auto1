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

-- Step 2: Export header + data to new file
-- Note: Using IDENTIFIER() to properly resolve variable in COPY INTO path
COPY INTO IDENTIFIER('@dev_data_ingress.finance.s3_test_finance_automation_output/' || $NEW_FILE_NAME)
FROM (
    -- Header row from header table
    SELECT header_line AS line
    FROM dev_data_ingress.finance.temp_header
    
    UNION ALL
    
    -- Data rows from existing file
    SELECT $1 || ',' || $2 || ',' || $3 || ',' || $4 || ',' || $5 || ',' ||
           $6 || ',' || $7 || ',' || $8 || ',' || $9 || ',' || $10 || ',' ||
           $11 || ',' || $12 || ',' || $13 || ',' || $14 || ',' || $15 || ',' ||
           $16 || ',' || $17 || ',' || $18 || ',' || $19 || ',' || $20 || ',' ||
           $21 || ',' || $22 || ',' || $23 || ',' || $24 || ',' || $25 || ',' ||
           $26 || ',' || $27 || ',' || $28 || ',' || $29 || ',' || $30 || ',' ||
           $31 || ',' || $32 || ',' || $33 || ',' || $34 || ',' || $35 || ',' ||
           $36 || ',' || $37 || ',' || $38 || ',' || $39 || ',' || $40 || ',' ||
           $41 || ',' || $42 || ',' || $43 AS line
    FROM IDENTIFIER('@dev_data_ingress.finance.s3_test_finance_automation_output/' || $FILE_NAME)
)
FILE_FORMAT = (TYPE = 'CSV' 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
               NULL_IF = ('NULL', 'null', ''))
SINGLE = TRUE
OVERWRITE = TRUE;

-- Step 3: Verify the new file
SELECT 
    'Headers added successfully' AS status,
    $NEW_FILE_NAME AS new_file_name,
    $FILE_NAME AS original_file_name;

-- Step 4: List the new file
LIST @dev_data_ingress.finance.s3_test_finance_automation_output 
PATTERN = $NEW_FILE_NAME;

-- Alternative: If IDENTIFIER() doesn't work, use this approach instead:
-- Replace the COPY INTO line above with:
-- COPY INTO '@dev_data_ingress.finance.s3_test_finance_automation_output/' || $NEW_FILE_NAME
-- And replace the FROM line with:
-- FROM '@dev_data_ingress.finance.s3_test_finance_automation_output/' || $FILE_NAME

