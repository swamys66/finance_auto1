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

**Option C: Using Python/Snowflake Connector**
See `01_load_csv_to_snowflake.py` for a Python script example.

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

```sql
-- Create view with dynamic prior month calculation
CREATE OR REPLACE VIEW dataeng_stage.public.view_partner_finance_mapped AS
SELECT 
    -- Mapping fields
    m.ID,
    m.Oracle_Customer_Name,
    m.Oracle_Customer_Name_ID,
    m.Oracle_Invoice_Group,
    m.Oracle_Invoice_Name,
    m.Oracle_GL_Account,
    
    -- Revenue aggregation fields
    r.*
    
FROM dataeng_stage.public.mapping_template_raw_CURSOR m
INNER JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    ON m.ID = r.ID
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
1. **Mapping Coverage**: What percentage of mapping records successfully joined
2. **Unmapped Records**: Which mapping records did not find a match
3. **Revenue Coverage**: What percentage of revenue records were mapped
4. **Unmapped Revenue**: Which revenue records did not get mapped
5. **Data Completeness**: Check for NULL values in critical joined fields

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
-- Unmapped mapping records
SELECT 
    m.*,
    'No matching revenue record found' AS unmapped_reason
FROM dataeng_stage.public.mapping_template_raw_CURSOR m
LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    ON m.ID = r.ID
    AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
WHERE r.ID IS NULL;

-- Unmapped revenue records
SELECT 
    r.*,
    'No matching mapping record found' AS unmapped_reason
FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
    ON r.ID = m.ID
WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
    AND m.ID IS NULL;
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

## Step 7: Automation Considerations

### 7.1 Monthly Automation Script
Consider creating an Airflow DAG or scheduled job that:
1. Automatically determines the prior month
2. Loads the latest CSV mapping file
3. Runs all data quality checks
4. Creates/updates the merged view
5. Exports to S3 with timestamped filenames
6. Sends notification with data quality summary

### 7.2 Error Handling
- Validate CSV file format before loading
- Check data quality thresholds before proceeding
- Alert on high unmapped record percentages
- Verify S3 export completion

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
- `01_load_csv_to_snowflake.py` - Python script for CSV loading
- `03_data_quality_checks_raw.sql` - Raw data quality validation
- `04_create_mapped_view.sql` - View creation script
- `05_data_quality_checks_merged.sql` - Merged data quality validation
- `06_export_to_s3.sql` - S3 export script

