# DBT Implementation for S3 Import

This directory contains the dbt (data build tool) implementation for importing CSV mapping files from S3 into Snowflake.

## Files

- `11_dbt_import_from_s3.sql` - Main dbt model for importing from S3
- `11_dbt_macros.sql` - Reusable macros for S3 operations
- `11_dbt_models.yml` - Model documentation and tests
- `11_dbt_vars.yml` - Configuration variables

## Setup

### 1. Add to dbt_project.yml

Add these variables to your `dbt_project.yml`:

```yaml
vars:
  s3_mapping_stage: "dataeng_stage.public.s3_mapping_import"
  s3_mapping_bucket_url: "s3://your-bucket-name/mapping-files/"
  s3_mapping_file_pattern: ".*mapping.*\\.csv"
```

### 2. Configure AWS Credentials

**Option A: Environment Variables (Recommended)**
```bash
export AWS_ACCESS_KEY_ID=your-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-key
```

**Option B: dbt Cloud Connections**
- Configure AWS credentials in dbt Cloud connection settings

**Option C: IAM Role (Best for Production)**
- Set up IAM role in Snowflake
- Update macro to use `AWS_ROLE` instead of key/secret

### 3. Add Macros to Your Project

Copy the macros from `11_dbt_macros.sql` to your dbt project's `macros/` directory, or include this file in your macros path.

## Usage

### Run the Model

```bash
# Run the model
dbt run --select mapping_template_raw_cursor

# Run with full refresh
dbt run --select mapping_template_raw_cursor --full-refresh

# Run with specific variables
dbt run --select mapping_template_raw_cursor --vars '{"s3_mapping_file_pattern": "mapping_202501.csv"}'
```

### Test the Model

```bash
# Run tests
dbt test --select mapping_template_raw_cursor

# Run specific test
dbt test --select mapping_template_raw_cursor --test-name unique
```

### View Documentation

```bash
# Generate and serve docs
dbt docs generate
dbt docs serve
```

## How It Works

1. **Pre-hooks**:
   - `create_s3_mapping_stage()`: Creates/verifies the S3 external stage
   - `truncate_mapping_table()`: Clears existing data before load

2. **Model Execution**:
   - Reads CSV files from S3 using the external stage
   - Applies pattern matching to find the correct file(s)
   - Loads data into the table with proper type casting

3. **Post-hooks**:
   - `validate_import()`: Validates row count, uniqueness, and NULL checks

## Configuration Options

### File Pattern Matching

Load from files matching a pattern:
```yaml
vars:
  s3_mapping_file_pattern: ".*mapping.*\\.csv"
```

### Specific File

To load a specific file, modify the model to use:
```sql
FROM @{{ var('s3_mapping_stage') }}/specific_file.csv
```

### Latest File by Date

Use a macro to select the latest file:
```sql
FROM (
    SELECT $1, $2, $3, $4, $5, $6
    FROM @{{ var('s3_mapping_stage') }}/
    (FILE_FORMAT => 'CSV', PATTERN => '.*mapping.*\\.csv')
    ORDER BY METADATA$FILE_LAST_MODIFIED DESC
    LIMIT 1
)
```

## Integration with Airflow

Use with the existing Airflow DAG:

```python
from cosmos.operators import DbtRunOperator

import_mapping = DbtRunOperator(
    task_id="import_mapping_from_s3",
    project_dir="/path/to/dbt/project",
    select="mapping_template_raw_cursor",
    vars={
        "s3_mapping_file_pattern": ".*mapping.*\\.csv"
    }
)
```

## Advantages of DBT Approach

1. **Version Control**: SQL code is versioned in git
2. **Testing**: Built-in data quality tests
3. **Documentation**: Auto-generated documentation
4. **Lineage**: Visual data lineage in dbt
5. **Reusability**: Macros can be reused across projects
6. **Dependencies**: Automatic dependency management
7. **Incremental Loads**: Can be configured for incremental loading

## Troubleshooting

### Stage Not Found
- Verify AWS credentials are correct
- Check S3 bucket path is accessible
- Ensure Snowflake has permissions to access S3

### No Files Found
- Verify file pattern matches files in S3
- Check file naming convention
- Review S3 bucket path

### Data Quality Issues
- Review validation logs in dbt output
- Check source CSV file format
- Verify column order matches expected structure

## Next Steps

After importing, the data can be used in downstream dbt models:
- Staging models for data cleaning
- Intermediate models for transformations
- Final models for business logic

Example downstream model:
```sql
-- models/staging/stg_finance_mapping.sql
SELECT
    ID,
    TRIM(Oracle_Customer_Name) AS customer_name,
    Oracle_GL_Account AS gl_account
FROM {{ ref('mapping_template_raw_cursor') }}
WHERE ID IS NOT NULL
```

