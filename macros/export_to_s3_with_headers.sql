-- DBT Macro: export_to_s3_with_headers
-- Enhanced version of export_to_s3 that includes CSV headers
-- 
-- This macro:
-- 1. Exports data from a table/view to S3 stage
-- 2. Includes header row as first line of CSV
-- 3. Supports dynamic timestamp in filename
-- 4. No compression (for smaller datasets)
-- 5. Single file export
--
-- Usage in dbt models:
--   post_hook: ["{{ export_to_s3_with_headers('stage_name', 'file_prefix', 'table_name', 'order_column', 'data_month_column') }}"]
--
-- Example:
--   {{ export_to_s3_with_headers('dev_data_ingress.finance.s3_test_finance_automation_output', 'partner_finance_mapped', 'dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping', 'ID', 'data_month') }}

{% macro export_to_s3_with_headers(stage_name, file_prefix, source_table, order_by_column='ID', data_month_column='data_month', overwrite=true) %}
    {# 
    Macro to export data from a table/view to S3 using COPY INTO with headers and dynamic timestamp
    
    Parameters:
    - stage_name: Full stage name
    - file_prefix: File name prefix
    - source_table: Full table/view name to export from
    - order_by_column: Column to order by (default: 'ID')
    - data_month_column: Column name containing the data_month value (default: 'data_month')
    - overwrite: Whether to overwrite existing files (default: true)
    #}
    
    {# Extract data_month from source table dynamically #}
    {% set data_month_sql %}
    SELECT DISTINCT TO_CHAR({{ data_month_column }}, 'YYYYMM') AS month_str
    FROM {{ source_table }}
    WHERE {{ data_month_column }} IS NOT NULL
    ORDER BY month_str DESC
    LIMIT 1
    {% endset %}
    
    {% set data_month_result = run_query(data_month_sql) %}
    {% if execute %}
        {% set month_str = data_month_result.columns[0].values()[0] %}
        {% set file_name = file_prefix ~ '_' ~ month_str ~ '.csv' %}
    {% else %}
        {% set file_name = file_prefix ~ '_YYYYMM.csv' %}
        {% set month_str = 'YYYYMM' %}
    {% endif %}
    
    {# Get column names for header row #}
    {% set table_parts = source_table.split('.') %}
    {% set database_name = table_parts[0] %}
    {% set schema_name = table_parts[1] %}
    {% set table_name_only = table_parts[2] %}
    
    {# Try to get column names - use LIMIT 0 query which is more reliable for views #}
    {% set get_columns_sql %}
    SELECT * FROM {{ source_table }} LIMIT 0
    {% endset %}
    
    {# Get column names from INFORMATION_SCHEMA (works for both tables and views) #}
    {% set get_columns_sql %}
    SELECT LISTAGG(COLUMN_NAME, ',') WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS col_list
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = UPPER('{{ database_name }}')
      AND TABLE_SCHEMA = UPPER('{{ schema_name }}')
      AND TABLE_NAME = UPPER('{{ table_name_only }}')
    {% endset %}
    
    {% set cols_result = run_query(get_columns_sql) %}
    {% set col_names = '' %}
    {% set col_array = [] %}
    
    {% if execute and cols_result %}
        {% if cols_result.columns and cols_result.columns[0] and cols_result.columns[0].values() %}
            {% set first_value = cols_result.columns[0].values()[0] %}
            {% if first_value %}
                {% set col_names = first_value %}
                {% set col_array = col_names.split(',') | map('trim') %}
                {{ log("Found " ~ col_array|length ~ " columns: " ~ col_names, info=True) }}
            {% else %}
                {{ log("WARNING: Column list is empty for " ~ source_table, info=True) }}
            {% endif %}
        {% else %}
            {{ log("WARNING: Cannot access column results for " ~ source_table, info=True) }}
        {% endif %}
    {% else %}
        {{ log("WARNING: Query execution failed for column detection on " ~ source_table, info=True) }}
    {% endif %}
    
    {% if col_array and col_array|length > 0 %}
        {# Create header select with quoted uppercase column names #}
        {% set header_select = col_array | map('upper') | map('quote') | join(',') %}
        {# Create data select with original column names cast to VARCHAR #}
        {% set data_select_parts = [] %}
        {% for col in col_array %}
            {% set _ = data_select_parts.append(col ~ '::VARCHAR') %}
        {% endfor %}
        {% set data_select = data_select_parts | join(',') %}
        {% set has_headers = true %}
        {{ log("Header row will be included with " ~ col_array|length ~ " columns", info=True) }}
    {% else %}
        {% set has_headers = false %}
        {{ log("WARNING: Cannot get column names, exporting without headers", info=True) }}
    {% endif %}
    
    {# Remove existing file if overwrite is true #}
    {% if overwrite %}
        {% set remove_sql %}
        REMOVE @{{ stage_name }}/{{ file_name }}
        {% endset %}
        {% do run_query(remove_sql) %}
        {{ log("Removed existing file: " ~ file_name, info=True) }}
    {% endif %}
    
    {# Export to S3 with headers #}
    {% if has_headers %}
        {% set export_sql %}
        COPY INTO @{{ stage_name }}/{{ file_name }}
        FROM (
            -- Header row: Column names as string literals
            SELECT {{ header_select }}
            FROM {{ source_table }}
            WHERE 1=0  -- Ensures we only get header structure
            
            UNION ALL
            
            -- Data rows: Cast all columns to VARCHAR to match header row types
            SELECT {{ data_select }}
            FROM {{ source_table }}
            {% if order_by_column %}
            ORDER BY {{ order_by_column }}
            {% endif %}
        )
        FILE_FORMAT = (TYPE = 'CSV' 
                       FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                       NULL_IF = ('NULL', 'null', '')
                       ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                       ESCAPE_UNENCLOSED_FIELD = '\\'
                       REPLACE_INVALID_CHARACTERS = TRUE)
        SINGLE = TRUE
        OVERWRITE = TRUE
        ;
        {% endset %}
    {% else %}
        {# Fallback: Export without headers #}
        {% set export_sql %}
        COPY INTO @{{ stage_name }}/{{ file_name }}
        FROM (
            SELECT * 
            FROM {{ source_table }}
            {% if order_by_column %}
            ORDER BY {{ order_by_column }}
            {% endif %}
        )
        FILE_FORMAT = (TYPE = 'CSV' 
                       FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                       NULL_IF = ('NULL', 'null', '')
                       ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                       ESCAPE_UNENCLOSED_FIELD = '\\'
                       REPLACE_INVALID_CHARACTERS = TRUE)
        SINGLE = TRUE
        OVERWRITE = TRUE
        ;
        {% endset %}
    {% endif %}
    
    {% do run_query(export_sql) %}
    {{ log("Data exported to S3: " ~ file_name ~ " from " ~ source_table ~ " (data_month: " ~ month_str ~ ")" ~ (" with headers" if has_headers else " without headers"), info=True) }}
{% endmacro %}

