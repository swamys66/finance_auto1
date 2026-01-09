# Finance Revenue Mapping Process

## Overview
This document outlines the complete process for loading CSV mapping data, joining it with partner finance revenue data, performing data quality checks, and exporting to S3.

## Prerequisites
- Access to Snowflake with permissions on:
  - `dataeng_stage.public` schema (or appropriate staging schema)
  - `BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION` view
- CSV file with required columns (see Step 1)
- S3 bucket and credentials configured (for Step 7)

---

## Step 1: Load CSV File into Snowflake

### 1.1 Prepare CSV File
Ensure your CSV file contains the following columns in this exact order:
1. `ID` (VARCHAR)
2. `Oracle_Customer_Name` (VARCHAR)
3. `Oracle_Customer_Name_ID` (VARCHAR)
4. `Oracle_Invoice_Group` (VARCHAR)
5. `Oracle_Invoice_Name` (VARCHAR)
6. `Oracle_GL_Account` (VARCHAR)

### 1.2 Load Options

**Option A: Using Snowflake Web UI**
1. Navigate to Snowflake Web UI
2. Select database: `dataeng_stage` (or your target database)
3. Select schema: `public`
4. Click "Tables" → "Create Table" → "From File"
5. Upload your CSV file
6. Name the table: `mapping_template_raw_CURSOR`

**Option B: Using SnowSQL/CLI**
```sql
-- Create table structure first
CREATE OR REPLACE TABLE dataeng_stage.public.mapping_template_raw_CURSOR (
    ID VARCHAR,
    Oracle_Customer_Name VARCHAR,
    Oracle_Customer_Name_ID VARCHAR,
    Oracle_Invoice_Group VARCHAR,
    Oracle_Invoice_Name VARCHAR,
    Oracle_GL_Account VARCHAR
);

-- Then use COPY INTO command
COPY INTO dataeng_stage.public.mapping_template_raw_CURSOR
FROM @your_stage_name/mapping_file.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

**Option C: Loading CSV from S3 Bucket**
This option loads the CSV file directly from an S3 bucket into Snowflake.

See `02_import_from_s3.sql` for the complete SQL script with all options and verification queries.

**Prerequisites:**
- S3 bucket with the CSV file
- AWS credentials configured in Snowflake
- External stage created (or use existing stage)

**Step 1: Create External Stage (if not already exists)**
```sql
-- Create external stage for S3
CREATE OR REPLACE STAGE dataeng_stage.public.s3_mapping_import
    URL = 's3://your-bucket-name/mapping-files/'
    CREDENTIALS = (
        AWS_KEY_ID = 'your-aws-access-key-id'
        AWS_SECRET_KEY = 'your-aws-secret-access-key'
    )
    FILE_FORMAT = (TYPE = 'CSV' 
                   SKIP_HEADER = 1 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                   ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE);

-- Alternative: Use IAM Role (if configured in Snowflake)
/*
CREATE OR REPLACE STAGE dataeng_stage.public.s3_mapping_import
    URL = 's3://your-bucket-name/mapping-files/'
    CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::123456789012:role/snowflake-role')
    FILE_FORMAT = (TYPE = 'CSV' 
                   SKIP_HEADER = 1 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"');
*/
```

**Step 2: Create Table Structure**
```sql
CREATE OR REPLACE TABLE dataeng_stage.public.mapping_template_raw_CURSOR (
    ID VARCHAR,
    Oracle_Customer_Name VARCHAR,
    Oracle_Customer_Name_ID VARCHAR,
    Oracle_Invoice_Group VARCHAR,
    Oracle_Invoice_Name VARCHAR,
    Oracle_GL_Account VARCHAR
);
```

**Step 3: Load Data from S3**
```sql
-- Load from specific file
COPY INTO dataeng_stage.public.mapping_template_raw_CURSOR
FROM @dataeng_stage.public.s3_mapping_import/mapping_file.csv
FILE_FORMAT = (TYPE = 'CSV' 
               SKIP_HEADER = 1 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"'
               ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR = 'ABORT_STATEMENT';

-- Or load from pattern (e.g., latest file with timestamp)
/*
COPY INTO dataeng_stage.public.mapping_template_raw_CURSOR
FROM @dataeng_stage.public.s3_mapping_import/
FILE_FORMAT = (TYPE = 'CSV' 
               SKIP_HEADER = 1 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"')
PATTERN = '.*mapping.*\\.csv'
ON_ERROR = 'ABORT_STATEMENT';
*/
```

**Step 4: Verify Load**
```sql
-- Check row count
SELECT COUNT(*) AS loaded_rows 
FROM dataeng_stage.public.mapping_template_raw_CURSOR;

-- Preview data
SELECT * 
FROM dataeng_stage.public.mapping_template_raw_CURSOR 
LIMIT 10;
```

**Option D: Using Python/Snowflake Connector**
See `01_load_csv_to_snowflake.py` for a Python script example.

**Option E: Using DBT (Data Build Tool)**
See `11_dbt_README.md` for complete dbt implementation. This approach provides:
- Version-controlled SQL code
- Built-in data quality tests
- Automatic documentation
- Integration with dbt workflows

**Quick Start:**
```bash
# Run dbt model
dbt run --select mapping_template_raw_cursor

# Run tests
dbt test --select mapping_template_raw_cursor
```

See `11_dbt_import_from_s3.sql` and `11_dbt_macros.sql` for implementation details.

---

## Step 2: Verify Table Structure

```sql
-- Verify table was created with correct structure
DESCRIBE TABLE dataeng_stage.public.mapping_template_raw_CURSOR;

-- Check row count
SELECT COUNT(*) AS total_rows 
FROM dataeng_stage.public.mapping_template_raw_CURSOR;
```

---

## Step 3: Data Quality Checks for Raw Mapping Data

### 3.1 Run Data Quality Validation Queries

See `03_data_quality_checks_raw.sql` for complete validation queries.

**Key Checks:**
1. **Uniqueness Check**: Verify ID is unique
2. **Completeness Check**: Check for NULL values in critical fields
3. **Data Type Validation**: Ensure data types are correct
4. **Duplicate Detection**: Identify any duplicate records
5. **Data Range Validation**: Check for unexpected values

### 3.2 Data Quality Summary Query

```sql
-- Comprehensive data quality check
SELECT 
    'Total Records' AS check_type,
    COUNT(*) AS check_value,
    NULL AS check_status
FROM dataeng_stage.public.mapping_template_raw_CURSOR

UNION ALL

SELECT 
    'Unique IDs' AS check_type,
    COUNT(DISTINCT ID) AS check_value,
    CASE 
        WHEN COUNT(DISTINCT ID) = COUNT(*) THEN 'PASS'
        ELSE 'FAIL - Duplicates found'
    END AS check_status
FROM dataeng_stage.public.mapping_template_raw_CURSOR

UNION ALL

SELECT 
    'NULL IDs' AS check_type,
    COUNT(*) AS check_value,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL - NULL IDs found'
    END AS check_status
FROM dataeng_stage.public.mapping_template_raw_CURSOR
WHERE ID IS NULL;
```

---

## Step 4: Create Mapping View with Prior Month Filter

### 4.1 Determine Prior Month
The process should filter for the prior month. For example:
- Current month: January 2026 → Filter: `data_month = '2025-12-01'`
- Current month: February 2026 → Filter: `data_month = '2026-01-01'`

### 4.2 Create Merged View

See `04_create_mapped_view.sql` for the complete view creation script.

**Important:** The view uses a LEFT JOIN with `BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION` as the master table. This ensures:
- **All revenue records** from the prior month are included in the view
- **Mapping fields** are populated when a mapping exists for the revenue record
- **Mapping fields are NULL** when no mapping exists (revenue records are still included)

```sql
-- Create view with dynamic prior month calculation
-- Master: BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION
-- LEFT JOIN to mapping_template_raw_CURSOR to include all revenue records
CREATE OR REPLACE VIEW dataeng_stage.public.view_partner_finance_mapped AS
SELECT 
    -- All fields from revenue aggregation view (master)
    r.*,
    
    -- Mapping fields from mapping_template_raw_CURSOR (may be NULL if no mapping)
    m.Oracle_Customer_Name,
    m.Oracle_Customer_Name_ID,
    m.Oracle_Invoice_Group,
    m.Oracle_Invoice_Name,
    m.Oracle_GL_Account
    
FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
    ON r.ID = m.ID
WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
    -- Alternative: Use specific date
    -- WHERE r.data_month = '2025-12-01'
;
```

---

## Step 5: Data Quality Checks for Merged View

### 5.1 Mapping Success Analysis

See `05_data_quality_checks_merged.sql` for complete validation queries.

**Key Checks:**
1. **Revenue Coverage**: What percentage of revenue records were successfully mapped
2. **Unmapped Revenue**: Which revenue records did not get a mapping (mapping fields will be NULL)
3. **Mapping Usage**: What percentage of mapping records were used (some mappings may not have revenue)
4. **Unused Mappings**: Which mapping records did not find a matching revenue record
5. **Data Completeness**: Check for NULL values in mapping fields (expected for unmapped revenue)

### 5.2 Mapping Summary Query

```sql
-- Mapping success analysis
WITH mapping_stats AS (
    SELECT 
        COUNT(DISTINCT m.ID) AS total_mapping_records,
        COUNT(DISTINCT CASE WHEN r.ID IS NOT NULL THEN m.ID END) AS mapped_records,
        COUNT(DISTINCT CASE WHEN r.ID IS NULL THEN m.ID END) AS unmapped_mapping_records
    FROM dataeng_stage.public.mapping_template_raw_CURSOR m
    LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
        ON m.ID = r.ID
        AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
),
revenue_stats AS (
    SELECT 
        COUNT(DISTINCT r.ID) AS total_revenue_records,
        COUNT(DISTINCT CASE WHEN m.ID IS NOT NULL THEN r.ID END) AS mapped_revenue_records,
        COUNT(DISTINCT CASE WHEN m.ID IS NULL THEN r.ID END) AS unmapped_revenue_records
    FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
        ON r.ID = m.ID
    WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
)
SELECT 
    ms.total_mapping_records,
    ms.mapped_records,
    ms.unmapped_mapping_records,
    ROUND(ms.mapped_records * 100.0 / NULLIF(ms.total_mapping_records, 0), 2) AS mapping_success_rate_pct,
    rs.total_revenue_records,
    rs.mapped_revenue_records,
    rs.unmapped_revenue_records,
    ROUND(rs.mapped_revenue_records * 100.0 / NULLIF(rs.total_revenue_records, 0), 2) AS revenue_coverage_rate_pct
FROM mapping_stats ms
CROSS JOIN revenue_stats rs;
```

### 5.3 Identify Unmapped Records

```sql
-- Unmapped revenue records (revenue exists but no mapping found)
-- These records will appear in the view with NULL mapping fields
SELECT 
    r.*,
    'No matching mapping record found' AS unmapped_reason
FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
    ON r.ID = m.ID
WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
    AND m.ID IS NULL;

-- Unused mapping records (mapping exists but no revenue for prior month)
-- These mappings are not used in the current view
SELECT 
    m.*,
    'No matching revenue record found for prior month' AS unmapped_reason
FROM dataeng_stage.public.mapping_template_raw_CURSOR m
LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    ON m.ID = r.ID
    AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
WHERE r.ID IS NULL;
```

---

## Step 6: Export to S3 Bucket

### 6.1 Prerequisites
- S3 bucket created and accessible
- AWS credentials configured in Snowflake
- External stage created (if not already exists)

### 6.2 Create External Stage (if needed)

```sql
-- Create external stage for S3
CREATE OR REPLACE STAGE dataeng_stage.public.s3_finance_export
    URL = 's3://your-bucket-name/finance-revenue-exports/'
    CREDENTIALS = (
        AWS_KEY_ID = 'your-aws-key-id'
        AWS_SECRET_KEY = 'your-aws-secret-key'
    )
    FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP' FIELD_OPTIONALLY_ENCLOSED_BY = '"' HEADER = TRUE);
```

### 6.3 Export View to S3

```sql
-- Export the mapped view to S3
COPY INTO @dataeng_stage.public.s3_finance_export/partner_finance_mapped_
FROM (
    SELECT * 
    FROM dataeng_stage.public.view_partner_finance_mapped
)
FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP' FIELD_OPTIONALLY_ENCLOSED_BY = '"' HEADER = TRUE)
OVERWRITE = TRUE
SINGLE = FALSE
MAX_FILE_SIZE = 5368709120; -- 5GB per file
```

### 6.4 Verify Export

```sql
-- List files in S3 stage
LIST @dataeng_stage.public.s3_finance_export;

-- Verify file count and sizes
SELECT 
    METADATA$FILENAME AS file_name,
    METADATA$FILE_ROW_NUMBER AS row_count,
    METADATA$FILE_CONTENT_KEY AS content_key
FROM @dataeng_stage.public.s3_finance_export
ORDER BY METADATA$FILENAME;
```

### 6.5 Alternative: Export as Single File

```sql
-- Export as single compressed file
COPY INTO @dataeng_stage.public.s3_finance_export/partner_finance_mapped_YYYYMMDD.csv.gz
FROM (
    SELECT * 
    FROM dataeng_stage.public.view_partner_finance_mapped
)
FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP' FIELD_OPTIONALLY_ENCLOSED_BY = '"' HEADER = TRUE)
SINGLE = TRUE
OVERWRITE = TRUE;
```

---

## Step 7: Automation

### 7.1 Automation Options

See `07_automation_guide.md` for comprehensive automation options including:
- **Airflow DAG** (Recommended for Enterprise) - See `08_finance_revenue_automation_dag.py`
- **Snowflake Tasks** (Native Snowflake Scheduling) - See `09_snowflake_tasks.sql`
- **Python Script with Cron** (Simple Automation) - See `10_automated_import_script.py`
- **Cloud Functions** (Event-driven, Serverless)

### 7.2 Quick Start: Python Script

For simple automation, use the Python script:

```bash
# Install dependencies
pip install snowflake-connector-python

# Set environment variables
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ACCOUNT=your_account

# Run all steps
python 10_automated_import_script.py

# Run specific step
python 10_automated_import_script.py --step import

# Dry run (test without executing)
python 10_automated_import_script.py --dry-run
```

### 7.3 Quick Start: Snowflake Tasks

For native Snowflake scheduling:

```sql
-- Execute the task setup script
-- This creates scheduled tasks that run automatically
-- See 09_snowflake_tasks.sql for complete setup
```

### 7.4 Quick Start: Airflow DAG

For enterprise automation with Airflow:

1. Copy `08_finance_revenue_automation_dag.py` to your Airflow DAGs folder
2. Configure Snowflake connection in Airflow
3. Adjust schedule and parameters as needed
4. Deploy and monitor in Airflow UI

### 7.5 Error Handling
- Validate CSV file format before loading
- Check data quality thresholds before proceeding
- Alert on high unmapped record percentages
- Verify S3 export completion
- Automatic retries on failure
- Email notifications on errors

---

## Monitoring and Maintenance

### Regular Checks
- Monitor mapping success rates monthly
- Review unmapped records and update mapping table as needed
- Verify S3 exports are accessible and complete
- Track data quality metrics over time

### Troubleshooting
- If mapping rate is low, review join key (ID) for data quality issues
- If export fails, check S3 credentials and permissions
- If data quality checks fail, review source CSV for issues

---

## Related Files

### Core Process Files
- `01_load_csv_to_snowflake.py` - Python script for CSV loading
- `02_import_from_s3.sql` - SQL script for importing CSV from S3 bucket
- `03_data_quality_checks_raw.sql` - Raw data quality validation
- `04_create_mapped_view.sql` - View creation script
- `05_data_quality_checks_merged.sql` - Merged data quality validation
- `06_export_to_s3.sql` - S3 export script

### Automation Files
- `07_automation_guide.md` - Comprehensive automation guide with all options
- `08_finance_revenue_automation_dag.py` - Airflow DAG for complete automation
- `09_snowflake_tasks.sql` - Snowflake native task scheduling
- `10_automated_import_script.py` - Python script for cron/scheduler automation

### DBT Implementation Files
- `11_dbt_import_from_s3.sql` - DBT model for S3 import
- `11_dbt_macros.sql` - Reusable dbt macros for S3 operations
- `11_dbt_models.yml` - Model documentation and tests
- `11_dbt_vars.yml` - Configuration variables
- `11_dbt_README.md` - Complete dbt implementation guide
- `11_dbt_import_from_s3_simple.sql` - Alternative simpler dbt model

