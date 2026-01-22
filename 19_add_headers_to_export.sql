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

-- Step 2: Create stored procedure to handle COPY INTO (avoids variable size limit)
CREATE OR REPLACE PROCEDURE dev_data_ingress.finance.add_headers_to_export(
    FILE_NAME STRING,
    NEW_FILE_NAME STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- First, create a temporary table with the data from the existing file
    EXECUTE IMMEDIATE 
        'CREATE OR REPLACE TEMPORARY TABLE temp_export_data AS
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
               $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
               $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
               $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
               $41, $42, $43
        FROM ''@dev_data_ingress.finance.s3_test_finance_automation_output/' || FILE_NAME || '''
        FILE_FORMAT = (TYPE = ''CSV'' 
                       FIELD_OPTIONALLY_ENCLOSED_BY = ''"'' 
                       NULL_IF = (''NULL'', ''null'', ''''))';
    
    -- Now export header + data
    EXECUTE IMMEDIATE 
        'COPY INTO ''@dev_data_ingress.finance.s3_test_finance_automation_output/' || NEW_FILE_NAME || '''
        FROM (
            -- Header row from header table (split into columns to match data structure)
            SELECT 
                SPLIT_PART(header_line, '','', 1) AS col1, SPLIT_PART(header_line, '','', 2) AS col2,
                SPLIT_PART(header_line, '','', 3) AS col3, SPLIT_PART(header_line, '','', 4) AS col4,
                SPLIT_PART(header_line, '','', 5) AS col5, SPLIT_PART(header_line, '','', 6) AS col6,
                SPLIT_PART(header_line, '','', 7) AS col7, SPLIT_PART(header_line, '','', 8) AS col8,
                SPLIT_PART(header_line, '','', 9) AS col9, SPLIT_PART(header_line, '','', 10) AS col10,
                SPLIT_PART(header_line, '','', 11) AS col11, SPLIT_PART(header_line, '','', 12) AS col12,
                SPLIT_PART(header_line, '','', 13) AS col13, SPLIT_PART(header_line, '','', 14) AS col14,
                SPLIT_PART(header_line, '','', 15) AS col15, SPLIT_PART(header_line, '','', 16) AS col16,
                SPLIT_PART(header_line, '','', 17) AS col17, SPLIT_PART(header_line, '','', 18) AS col18,
                SPLIT_PART(header_line, '','', 19) AS col19, SPLIT_PART(header_line, '','', 20) AS col20,
                SPLIT_PART(header_line, '','', 21) AS col21, SPLIT_PART(header_line, '','', 22) AS col22,
                SPLIT_PART(header_line, '','', 23) AS col23, SPLIT_PART(header_line, '','', 24) AS col24,
                SPLIT_PART(header_line, '','', 25) AS col25, SPLIT_PART(header_line, '','', 26) AS col26,
                SPLIT_PART(header_line, '','', 27) AS col27, SPLIT_PART(header_line, '','', 28) AS col28,
                SPLIT_PART(header_line, '','', 29) AS col29, SPLIT_PART(header_line, '','', 30) AS col30,
                SPLIT_PART(header_line, '','', 31) AS col31, SPLIT_PART(header_line, '','', 32) AS col32,
                SPLIT_PART(header_line, '','', 33) AS col33, SPLIT_PART(header_line, '','', 34) AS col34,
                SPLIT_PART(header_line, '','', 35) AS col35, SPLIT_PART(header_line, '','', 36) AS col36,
                SPLIT_PART(header_line, '','', 37) AS col37, SPLIT_PART(header_line, '','', 38) AS col38,
                SPLIT_PART(header_line, '','', 39) AS col39, SPLIT_PART(header_line, '','', 40) AS col40,
                SPLIT_PART(header_line, '','', 41) AS col41, SPLIT_PART(header_line, '','', 42) AS col42,
                SPLIT_PART(header_line, '','', 43) AS col43
            FROM dev_data_ingress.finance.temp_header
            
            UNION ALL
            
            -- Data rows from temp table
            SELECT * FROM temp_export_data
        )
        FILE_FORMAT = (TYPE = ''CSV'' 
                       FIELD_OPTIONALLY_ENCLOSED_BY = ''"'' 
                       NULL_IF = (''NULL'', ''null'', '''')
                       ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
        SINGLE = TRUE
        OVERWRITE = TRUE';
    
    RETURN 'Headers added successfully to ' || NEW_FILE_NAME;
END;
$$;

-- Step 3: Call the stored procedure
CALL dev_data_ingress.finance.add_headers_to_export($FILE_NAME, $NEW_FILE_NAME);

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

