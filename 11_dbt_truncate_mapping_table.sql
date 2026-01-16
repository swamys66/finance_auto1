-- DBT Macro: truncate_mapping_table
-- Standalone macro file for truncating the mapping table before loading new data
-- 
-- This macro:
-- 1. Truncates (empties) the table while preserving its structure
-- 2. Uses IF EXISTS to prevent errors if table doesn't exist
-- 3. Logs the operation for debugging
--
-- Usage in dbt models:
--   pre_hook: ["{{ truncate_mapping_table() }}"]
--
-- Or call directly:
--   {{ truncate_mapping_table() }}

{% macro truncate_mapping_table() %}
    {# 
    Macro to truncate the mapping table before loading new data
    
    This ensures a clean load by removing all existing rows from the table
    while preserving the table structure. Safe to run even if table doesn't exist.
    
    Uses {{ this }} which refers to the current dbt model's table name.
    #}
    
    {% set truncate_sql %}
    TRUNCATE TABLE IF EXISTS {{ this }}
    {% endset %}
    
    {% do run_query(truncate_sql) %}
    {{ log("Mapping table truncated: " ~ this, info=True) }}
{% endmacro %}

