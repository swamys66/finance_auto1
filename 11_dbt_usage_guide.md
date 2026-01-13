# DBT Usage Guide: Using Macros for COPY INTO Instead of Manual SQL

This guide explains how to use the dbt macros to perform the COPY INTO operation automatically, replacing the need to manually run `02_import_from_s3.sql`.

## Overview

**Manual Approach (02_import_from_s3.sql):**
- Run SQL statements manually
- Execute COPY INTO command step-by-step
- Manual verification

**DBT Approach (11_dbt_import_from_s3.sql):**
- Automated execution via dbt run
- COPY INTO happens automatically in pre-hooks
- Built-in validation and testing

## How DBT Automatically Executes COPY INTO

The dbt model `11_dbt_import_from_s3.sql` uses **pre-hooks** to automatically execute the COPY INTO operation before the model runs.

### Current Implementation

```sql
{{
    config(
        materialized='table',
        schema='finance',
        database='dev_data_ingress',
        pre_hook=[
            "{{ truncate_mapping_table() }}",  -- Step 1: Clear existing data
            "{{ load_from_s3_pattern(...) }}"   -- Step 2: COPY INTO from S3
        ],
        post_hook=[
            "{{ validate_import() }}"            -- Step 3: Validate results
        ]
    )
}}
```

### What Happens When You Run `dbt run`

1. **Pre-hook 1: `truncate_mapping_table()`**
   - Executes: `TRUNCATE TABLE IF EXISTS dev_data_ingress.finance.mapping_template_raw_cursor`
   - Clears existing data

2. **Pre-hook 2: `load_from_s3_pattern()`**
   - Executes the COPY INTO command automatically:
   ```sql
   COPY INTO dev_data_ingress.finance.mapping_template_raw_cursor
   (
       ID, Oracle_Customer_Name, Oracle_Customer_Name_ID,
       Oracle_Invoice_Group, Oracle_Invoice_Name, Oracle_GL_Account
   )
   FROM @dev_data_ingress.finance.s3_test_finance_automation_input/
   FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 ...)
   PATTERN = '.*mapping.*\.csv'
   ON_ERROR = 'ABORT_STATEMENT';
   ```

3. **Model Execution**
   - The SELECT statement reads from the loaded table

4. **Post-hook: `validate_import()`**
   - Validates row count, uniqueness, and data quality

## Step-by-Step Setup

### Step 1: Add Macros to Your dbt Project

Copy `11_dbt_macros.sql` to your dbt project's `macros/` directory:

```bash
# Example structure
your_dbt_project/
  macros/
    11_dbt_macros.sql  # Copy the macros file here
  models/
    11_dbt_import_from_s3.sql  # Copy the model here
```

### Step 2: Configure Variables in dbt_project.yml

Add to your `dbt_project.yml`:

```yaml
vars:
  # S3 Stage (must already exist)
  s3_mapping_stage: "dev_data_ingress.finance.s3_test_finance_automation_input"
  
  # File pattern to match in S3
  s3_mapping_file_pattern: ".*mapping.*\\.csv"
  
  # Optional: Override for specific files
  # s3_mapping_file_pattern: "mapping_20250115.csv"
```

### Step 3: Run the DBT Model

```bash
# Basic run - uses default pattern
dbt run --select mapping_template_raw_cursor

# With custom file pattern
dbt run --select mapping_template_raw_cursor \
  --vars '{"s3_mapping_file_pattern": "mapping_20250115.csv"}'

# With full refresh (recreates table)
dbt run --select mapping_template_raw_cursor --full-refresh
```

## Comparison: Manual vs DBT

| Aspect | Manual (02_import_from_s3.sql) | DBT (11_dbt_import_from_s3.sql) |
|--------|--------------------------------|----------------------------------|
| **Execution** | Run SQL manually | `dbt run` command |
| **COPY INTO** | Manual step-by-step | Automatic via pre-hook macro |
| **Validation** | Manual queries | Automatic post-hook validation |
| **Testing** | Manual checks | `dbt test` with built-in tests |
| **Documentation** | Manual notes | Auto-generated docs |
| **Version Control** | SQL file | Git-tracked dbt model |
| **Error Handling** | Manual debugging | Automatic error reporting |
| **Dependencies** | Manual tracking | Automatic dependency graph |

## Using Macros Directly (Advanced)

You can also call the macros directly in other dbt models or operations:

### Option 1: Use in Another Model's Pre-hook

```sql
{{
    config(
        pre_hook=[
            "{{ load_from_s3_pattern('dev_data_ingress.finance.s3_test_finance_automation_input', '.*mapping.*\\.csv', 'dev_data_ingress.finance.mapping_template_raw_cursor') }}"
        ]
    )
}}

SELECT * FROM dev_data_ingress.finance.mapping_template_raw_cursor
```

### Option 2: Use Specific File Macro

```sql
{{
    config(
        pre_hook=[
            "{{ load_from_s3_file('dev_data_ingress.finance.s3_test_finance_automation_input', 'mapping_20250115.csv', 'dev_data_ingress.finance.mapping_template_raw_cursor') }}"
        ]
    )
}}
```

### Option 3: Create a Custom Macro That Uses COPY INTO

You can create your own macro that wraps the COPY INTO:

```sql
-- In your macros file
{% macro import_finance_mapping(file_pattern='.*mapping.*\\.csv') %}
    {{ load_from_s3_pattern(
        var('s3_mapping_stage', 'dev_data_ingress.finance.s3_test_finance_automation_input'),
        file_pattern,
        'dev_data_ingress.finance.mapping_template_raw_cursor'
    ) }}
{% endmacro %}
```

Then use it:
```sql
{{
    config(
        pre_hook=["{{ import_finance_mapping('mapping_20250115.csv') }}"]
    )
}}
```

## Complete Workflow Example

### 1. Setup (One-time)

```bash
# Ensure stage exists (one-time setup)
# Stage: dev_data_ingress.finance.s3_test_finance_automation_input

# Add macros to dbt project
cp 11_dbt_macros.sql your_dbt_project/macros/

# Add model to dbt project
cp 11_dbt_import_from_s3.sql your_dbt_project/models/
```

### 2. Configure

```yaml
# dbt_project.yml
vars:
  s3_mapping_stage: "dev_data_ingress.finance.s3_test_finance_automation_input"
  s3_mapping_file_pattern: ".*mapping.*\\.csv"
```

### 3. Run

```bash
# Run the model (automatically executes COPY INTO)
dbt run --select mapping_template_raw_cursor

# Check logs - you'll see:
# - "Mapping table truncated"
# - "Data loaded from S3 pattern: .*mapping.*\.csv"
# - "Import validation passed: X rows loaded"
```

### 4. Verify

```bash
# Run tests
dbt test --select mapping_template_raw_cursor

# Check in Snowflake
SELECT COUNT(*) FROM dev_data_ingress.finance.mapping_template_raw_cursor;
```

## Troubleshooting

### Issue: COPY INTO Not Executing

**Check:**
1. Macros file is in `macros/` directory
2. Model file is in `models/` directory
3. Pre-hooks are correctly configured

**Solution:**
```bash
# Verify macros are loaded
dbt list --select mapping_template_raw_cursor

# Check for syntax errors
dbt parse
```

### Issue: Pattern Not Matching Files

**Check:**
```sql
-- Verify files exist in stage
LIST @dev_data_ingress.finance.s3_test_finance_automation_input;
```

**Solution:**
```bash
# Use specific file pattern
dbt run --select mapping_template_raw_cursor \
  --vars '{"s3_mapping_file_pattern": "exact_filename.csv"}'
```

### Issue: Stage Not Found

**Error:** `Stage 'dev_data_ingress.finance.s3_test_finance_automation_input' does not exist`

**Solution:**
- Stage must be created as one-time setup
- Verify stage exists: `DESCRIBE STAGE dev_data_ingress.finance.s3_test_finance_automation_input;`

## Benefits of DBT Approach

1. **Automation**: No manual SQL execution needed
2. **Reproducibility**: Same process every time
3. **Testing**: Built-in data quality tests
4. **Documentation**: Auto-generated docs
5. **Lineage**: Visual data flow
6. **Integration**: Works with Airflow, dbt Cloud, CI/CD
7. **Version Control**: All code in git

## Next Steps

After using dbt to import, you can:
- Reference the table in other dbt models: `{{ ref('mapping_template_raw_cursor') }}`
- Build downstream transformations
- Create staging and final models
- Set up automated schedules

## Related Files

- `11_dbt_import_from_s3.sql` - Main model (uses macros automatically)
- `11_dbt_macros.sql` - Macros that execute COPY INTO
- `02_import_from_s3.sql` - Manual SQL version (for reference)

