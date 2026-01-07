-- Import CSV Mapping File from S3 Bucket
-- Table: dataeng_stage.public.mapping_template_raw_CURSOR

-- ============================================================================
-- STEP 1: Create External Stage (if not already exists)
-- ============================================================================
-- Update the following with your S3 bucket details:
-- - S3 bucket name and path
-- - AWS credentials (or use IAM role if configured)
-- - File format settings

CREATE OR REPLACE STAGE dataeng_stage.public.s3_mapping_import
    URL = 's3://your-bucket-name/mapping-files/'
    CREDENTIALS = (
        AWS_KEY_ID = 'your-aws-access-key-id'
        AWS_SECRET_KEY = 'your-aws-secret-access-key'
    )
    FILE_FORMAT = (TYPE = 'CSV' 
                   SKIP_HEADER = 1 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                   ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                   NULL_IF = ('NULL', 'null', ''));

-- Alternative: Use IAM Role (if configured in Snowflake)
/*
CREATE OR REPLACE STAGE dataeng_stage.public.s3_mapping_import
    URL = 's3://your-bucket-name/mapping-files/'
    CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::123456789012:role/snowflake-role')
    FILE_FORMAT = (TYPE = 'CSV' 
                   SKIP_HEADER = 1 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                   ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE);
*/

-- ============================================================================
-- STEP 2: Verify Stage Creation
-- ============================================================================
DESCRIBE STAGE dataeng_stage.public.s3_mapping_import;

-- List files in S3 stage
LIST @dataeng_stage.public.s3_mapping_import;

-- ============================================================================
-- STEP 3: Create Table Structure
-- ============================================================================
CREATE OR REPLACE TABLE dataeng_stage.public.mapping_template_raw_CURSOR (
    ID VARCHAR,
    Oracle_Customer_Name VARCHAR,
    Oracle_Customer_Name_ID VARCHAR,
    Oracle_Invoice_Group VARCHAR,
    Oracle_Invoice_Name VARCHAR,
    Oracle_GL_Account VARCHAR
);

-- ============================================================================
-- STEP 4: Load Data from S3 - Option A: Specific File
-- ============================================================================
-- Load from a specific file in S3
-- Replace 'mapping_file.csv' with your actual file name

COPY INTO dataeng_stage.public.mapping_template_raw_CURSOR
FROM @dataeng_stage.public.s3_mapping_import/mapping_file.csv
FILE_FORMAT = (TYPE = 'CSV' 
               SKIP_HEADER = 1 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"'
               ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
               NULL_IF = ('NULL', 'null', ''))
ON_ERROR = 'ABORT_STATEMENT'
PURGE = FALSE;

-- ============================================================================
-- STEP 5: Load Data from S3 - Option B: Pattern Matching
-- ============================================================================
-- Load from files matching a pattern (useful for timestamped files)
-- Uncomment and modify the pattern as needed

/*
COPY INTO dataeng_stage.public.mapping_template_raw_CURSOR
FROM @dataeng_stage.public.s3_mapping_import/
FILE_FORMAT = (TYPE = 'CSV' 
               SKIP_HEADER = 1 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"'
               ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
PATTERN = '.*mapping.*\\.csv'
ON_ERROR = 'ABORT_STATEMENT'
PURGE = FALSE;
*/

-- ============================================================================
-- STEP 6: Load Data from S3 - Option C: Latest File by Date
-- ============================================================================
-- Load the most recent file based on modification date
-- Uncomment and modify as needed

/*
COPY INTO dataeng_stage.public.mapping_template_raw_CURSOR
FROM (
    SELECT $1, $2, $3, $4, $5, $6
    FROM @dataeng_stage.public.s3_mapping_import
    (FILE_FORMAT => 'CSV', PATTERN => '.*mapping.*\\.csv')
    ORDER BY METADATA$FILE_LAST_MODIFIED DESC
    LIMIT 1
)
FILE_FORMAT = (TYPE = 'CSV' 
               SKIP_HEADER = 1 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"'
               ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR = 'ABORT_STATEMENT';
*/

-- ============================================================================
-- STEP 7: Verify Load - Check Row Count
-- ============================================================================
SELECT COUNT(*) AS loaded_rows 
FROM dataeng_stage.public.mapping_template_raw_CURSOR;

-- ============================================================================
-- STEP 8: Verify Load - Preview Data
-- ============================================================================
SELECT * 
FROM dataeng_stage.public.mapping_template_raw_CURSOR 
LIMIT 10;

-- ============================================================================
-- STEP 9: Verify Load - Check for Errors
-- ============================================================================
-- Check COPY history for any errors
SELECT 
    FILE_NAME,
    FILE_SIZE,
    ROW_COUNT,
    ROW_PARSED,
    FIRST_ERROR,
    FIRST_ERROR_LINE,
    FIRST_ERROR_COLUMN_NAME,
    FIRST_ERROR_CHARACTER_POS,
    STATUS
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MAPPING_TEMPLATE_RAW_CURSOR',
    START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- ============================================================================
-- STEP 10: Verify Load - Data Quality Quick Check
-- ============================================================================
SELECT 
    'Quick Data Quality Check' AS check_type,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT ID) AS unique_ids,
    COUNT(CASE WHEN ID IS NULL THEN 1 END) AS null_ids,
    COUNT(CASE WHEN Oracle_Customer_Name IS NULL THEN 1 END) AS null_customer_names,
    COUNT(CASE WHEN Oracle_GL_Account IS NULL THEN 1 END) AS null_gl_accounts,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT ID) AND COUNT(CASE WHEN ID IS NULL THEN 1 END) = 0 
        THEN 'PASS'
        ELSE 'FAIL - Check data quality'
    END AS status
FROM dataeng_stage.public.mapping_template_raw_CURSOR;

-- ============================================================================
-- TROUBLESHOOTING
-- ============================================================================
-- If load fails, check:
-- 1. S3 bucket exists and is accessible
-- 2. AWS credentials are correct
-- 3. Snowflake has permissions to read from S3
-- 4. File path in stage URL is correct
-- 5. CSV file format matches expected structure
-- 6. File exists in the specified S3 location

-- Test stage connectivity
/*
SELECT $1, $2, $3, $4, $5, $6
FROM @dataeng_stage.public.s3_mapping_import
(FILE_FORMAT => 'CSV', PATTERN => '.*mapping.*\\.csv')
LIMIT 5;
*/

-- Check file metadata
/*
SELECT 
    METADATA$FILENAME AS file_name,
    METADATA$FILE_ROW_NUMBER AS row_number,
    METADATA$FILE_CONTENT_KEY AS content_key,
    METADATA$FILE_LAST_MODIFIED AS last_modified
FROM @dataeng_stage.public.s3_mapping_import
(FILE_FORMAT => 'CSV')
LIMIT 10;
*/

