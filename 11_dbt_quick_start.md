# Quick Start: Using DBT for COPY INTO (Replaces 02_import_from_s3.sql)

## TL;DR - How to Use DBT Instead of Manual SQL

Instead of running `02_import_from_s3.sql` manually, use dbt:

```bash
# This automatically runs COPY INTO via macros
dbt run --select mapping_template_raw_cursor
```

## What Happens Automatically

When you run `dbt run --select mapping_template_raw_cursor`, the following happens **automatically**:

1. ✅ **Truncates table** (via `truncate_mapping_table()` macro)
2. ✅ **Runs COPY INTO** (via `load_from_s3_pattern()` macro) 
3. ✅ **Validates import** (via `validate_import()` macro)

**No manual SQL execution needed!**

## Setup (One-Time)

### 1. Copy Files to Your dbt Project

```bash
# Copy macros
cp 11_dbt_macros.sql your_dbt_project/macros/

# Copy model
cp 11_dbt_import_from_s3.sql your_dbt_project/models/staging/

# Copy model config
cp 11_dbt_models.yml your_dbt_project/models/staging/
```

### 2. Configure Variables

Add to `dbt_project.yml`:

```yaml
vars:
  s3_mapping_stage: "dev_data_ingress.finance.s3_test_finance_automation_input"
  s3_mapping_file_pattern: ".*mapping.*\\.csv"
```

## Usage

### Basic Run

```bash
# Automatically executes COPY INTO
dbt run --select mapping_template_raw_cursor
```

### With Custom File Pattern

```bash
# Load specific file
dbt run --select mapping_template_raw_cursor \
  --vars '{"s3_mapping_file_pattern": "mapping_20250115.csv"}'
```

### With Tests

```bash
# Run data quality tests
dbt test --select mapping_template_raw_cursor
```

## How It Works

The `load_from_s3_pattern()` macro in `11_dbt_macros.sql` automatically generates and executes:

```sql
COPY INTO dev_data_ingress.finance.mapping_template_raw_cursor
(ID, Oracle_Customer_Name, Oracle_Customer_Name_ID, 
 Oracle_Invoice_Group, Oracle_Invoice_Name, Oracle_GL_Account)
FROM @dev_data_ingress.finance.s3_test_finance_automation_input/
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 ...)
PATTERN = '.*mapping.*\.csv'
ON_ERROR = 'ABORT_STATEMENT';
```

**You don't need to write or run this SQL manually!**

## Comparison

| Task | Manual (02_import_from_s3.sql) | DBT (11_dbt_import_from_s3.sql) |
|------|-------------------------------|----------------------------------|
| Execute COPY INTO | Run SQL manually | `dbt run` command |
| Truncate table | Manual step | Automatic pre-hook |
| Validate | Manual queries | Automatic post-hook |
| Test data quality | Manual checks | `dbt test` command |

## Example Output

When you run `dbt run --select mapping_template_raw_cursor`, you'll see:

```
Running with dbt=1.x.x
Found 1 model, 0 tests, 0 sources, 0 exposures, 0 metrics

Concurrency: 1 threads (target='dev')

1 of 1 START table model dev_data_ingress.finance.mapping_template_raw_cursor [RUN]
  [Pre-hook] Mapping table truncated
  [Pre-hook] Data loaded from S3 pattern: .*mapping.*\.csv
1 of 1 OK created table model dev_data_ingress.finance.mapping_template_raw_cursor [SUCCESS in 5.2s]
  [Post-hook] Import validation passed: 1250 rows loaded

Completed successfully
```

## Troubleshooting

**Problem:** "Macro not found"
- **Solution:** Ensure `11_dbt_macros.sql` is in your `macros/` directory

**Problem:** "Stage not found"  
- **Solution:** Stage must exist. Verify: `DESCRIBE STAGE dev_data_ingress.finance.s3_test_finance_automation_input;`

**Problem:** "No files found"
- **Solution:** Check file pattern or list files: `LIST @dev_data_ingress.finance.s3_test_finance_automation_input;`

## Next Steps

After importing with dbt:
- Use in other models: `{{ ref('mapping_template_raw_cursor') }}`
- Build transformations
- Schedule with Airflow or dbt Cloud

