-- Export Merged View to S3 Bucket
-- View: dev_data_ingress.finance.view_partner_finance_mapped

-- ============================================================================
-- STEP 1: Create External Stage (if not already exists)
-- ============================================================================
-- Update the following with your S3 bucket details:
-- - S3 bucket name
-- - AWS credentials (or use IAM role if configured)
-- - File path prefix

CREATE OR REPLACE STAGE dev_data_ingress.finance.s3_finance_export
    URL = 's3://your-bucket-name/finance-revenue-exports/'
    CREDENTIALS = (
        AWS_KEY_ID = 'your-aws-access-key-id'
        AWS_SECRET_KEY = 'your-aws-secret-access-key'
    )
    FILE_FORMAT = (TYPE = 'CSV' 
                   COMPRESSION = 'GZIP' 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                   HEADER = TRUE
                   NULL_IF = ('NULL', 'null', ''));

-- Alternative: Use IAM Role (if configured in Snowflake)
/*
CREATE OR REPLACE STAGE dev_data_ingress.finance.s3_finance_export
    URL = 's3://your-bucket-name/finance-revenue-exports/'
    CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::123456789012:role/snowflake-role')
    FILE_FORMAT = (TYPE = 'CSV' 
                   COMPRESSION = 'GZIP' 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                   HEADER = TRUE);
*/

-- ============================================================================
-- STEP 2: Verify Stage Creation
-- ============================================================================
DESCRIBE STAGE dev_data_ingress.finance.s3_finance_export;

-- ============================================================================
-- STEP 3: Export View to S3 (Multiple Files - Recommended for Large Datasets)
-- ============================================================================
-- This will create multiple files if data exceeds MAX_FILE_SIZE
-- Files will be automatically named with unique identifiers

COPY INTO @dev_data_ingress.finance.s3_finance_export/partner_finance_mapped_
FROM (
    SELECT * 
    FROM dev_data_ingress.finance.view_partner_finance_mapped
    ORDER BY ID  -- Optional: Order data for consistency
)
FILE_FORMAT = (TYPE = 'CSV' 
               COMPRESSION = 'GZIP' 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
               HEADER = TRUE
               NULL_IF = ('NULL', 'null', ''))
OVERWRITE = TRUE
SINGLE = FALSE
MAX_FILE_SIZE = 5368709120;  -- 5GB per file (adjust as needed)

-- ============================================================================
-- STEP 4: Export View to S3 (Single File - For Smaller Datasets)
-- ============================================================================
-- Use this option if you want a single file with timestamp
-- Uncomment and modify the filename as needed

/*
COPY INTO @dev_data_ingress.finance.s3_finance_export/partner_finance_mapped_20251201.csv.gz
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
*/

-- ============================================================================
-- STEP 5: Export with Dynamic Timestamp (Recommended for Automation)
-- ============================================================================
-- This creates a file with the prior month in the filename
-- Example: partner_finance_mapped_202512.csv.gz

/*
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
*/

-- ============================================================================
-- STEP 6: Verify Export - List Files in S3 Stage
-- ============================================================================
LIST @dev_data_ingress.finance.s3_finance_export;

-- ============================================================================
-- STEP 7: Verify Export - Get File Details
-- ============================================================================
SELECT 
    METADATA$FILENAME AS file_name,
    METADATA$FILE_ROW_NUMBER AS row_count,
    METADATA$FILE_CONTENT_KEY AS content_key,
    METADATA$FILE_LAST_MODIFIED AS last_modified
FROM @dev_data_ingress.finance.s3_finance_export
ORDER BY METADATA$FILE_LAST_MODIFIED DESC;

-- ============================================================================
-- STEP 8: Verify Export - Sample Data from S3
-- ============================================================================
-- Read a sample of the exported data to verify format
SELECT *
FROM @dev_data_ingress.finance.s3_finance_export
(FILE_FORMAT => 'CSV', PATTERN => '.*partner_finance_mapped.*')
LIMIT 10;

-- ============================================================================
-- STEP 9: Export Summary Statistics
-- ============================================================================
WITH export_stats AS (
    SELECT 
        COUNT(*) AS total_files,
        SUM(METADATA$FILE_ROW_NUMBER) AS total_rows_exported,
        MIN(METADATA$FILE_LAST_MODIFIED) AS first_file_exported,
        MAX(METADATA$FILE_LAST_MODIFIED) AS last_file_exported
    FROM @dev_data_ingress.finance.s3_finance_export
    WHERE METADATA$FILENAME LIKE '%partner_finance_mapped%'
),
source_stats AS (
    SELECT COUNT(*) AS source_row_count
    FROM dev_data_ingress.finance.view_partner_finance_mapped
)
SELECT 
    'EXPORT VERIFICATION SUMMARY' AS report_section,
    es.total_files,
    es.total_rows_exported,
    ss.source_row_count,
    CASE 
        WHEN es.total_rows_exported = ss.source_row_count THEN 'PASS - All rows exported'
        ELSE 'WARNING - Row count mismatch'
    END AS export_status,
    es.first_file_exported,
    es.last_file_exported
FROM export_stats es
CROSS JOIN source_stats ss;

-- ============================================================================
-- STEP 10: Clean Up Old Exports (Optional)
-- ============================================================================
-- Remove files older than 90 days (adjust as needed)
/*
REMOVE @dev_data_ingress.finance.s3_finance_export
PATTERN = '.*partner_finance_mapped.*'
BEFORE = DATEADD(DAY, -90, CURRENT_DATE());
*/

-- ============================================================================
-- TROUBLESHOOTING
-- ============================================================================
-- If export fails, check:
-- 1. S3 bucket exists and is accessible
-- 2. AWS credentials are correct
-- 3. Snowflake has permissions to write to S3
-- 4. File path in stage URL is correct
-- 5. View has data (run: SELECT COUNT(*) FROM view_partner_finance_mapped)

-- Test stage connectivity
/*
SELECT $1, $2, $3
FROM @dev_data_ingress.finance.s3_finance_export
LIMIT 1;
*/

