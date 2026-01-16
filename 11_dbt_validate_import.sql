-- DBT Macro: validate_import
-- Standalone macro file for validating imported data after S3 load
-- 
-- This macro:
-- 1. Checks row count (ensures data was loaded)
-- 2. Validates uniqueness of ID column
-- 3. Checks for NULL IDs
-- 4. Logs warnings or raises errors based on validation results
--
-- Usage in dbt models:
--   post_hook: ["{{ validate_import() }}"]
--
-- Or call directly:
--   {{ validate_import() }}

{% macro validate_import() %}
    {# 
    Macro to validate that the import was successful
    
    Performs the following checks:
    - Row count: Ensures at least one row was loaded
    - Uniqueness: Ensures all IDs are unique (no duplicates)
    - NULL check: Warns if any NULL IDs are found
    
    Uses {{ this }} which refers to the current dbt model's table name.
    #}
    
    {% set validation_sql %}
    SELECT 
        COUNT(*) AS row_count,
        COUNT(DISTINCT ID) AS unique_ids,
        COUNT(CASE WHEN ID IS NULL THEN 1 END) AS null_ids
    FROM {{ this }}
    {% endset %}
    
    {% set results = run_query(validation_sql) %}
    
    {% if execute %}
        {% set row_count = results.columns[0].values()[0] %}
        {% set unique_count = results.columns[1].values()[0] %}
        {% set null_count = results.columns[2].values()[0] %}
        
        {% if row_count == 0 %}
            {{ exceptions.raise_compiler_error("Import validation failed: No rows loaded into " ~ this) }}
        {% elif row_count != unique_count %}
            {{ log("WARNING: Duplicate IDs found in imported data. Total rows: " ~ row_count ~ ", Unique IDs: " ~ unique_count, info=True) }}
        {% elif null_count > 0 %}
            {{ log("WARNING: NULL IDs found in imported data. NULL count: " ~ null_count, info=True) }}
        {% else %}
            {{ log("Import validation passed: " ~ row_count ~ " rows loaded into " ~ this, info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}

