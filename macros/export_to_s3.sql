-- DBT Macro: export_to_s3
-- Standalone macro file for exporting data to S3 using COPY INTO
-- 
-- This macro:
-- 1. Exports data from a table/view to S3 stage
-- 2. Supports dynamic timestamp in filename (like 6th program)
-- 3. No compression (for smaller datasets)
-- 4. Single file export
--
-- Usage in dbt models:
--   post_hook: ["{{ export_to_s3('stage_name', 'file_prefix', 'table_name', 'order_column', 'data_month_column') }}"]
--
-- Example:
--   {{ export_to_s3('dev_data_ingress.finance.s3_test_finance_automation_output', 'partner_finance_mapped', 'dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping', 'ID', 'data_month') }}

{% macro export_to_s3(stage_name, file_prefix, source_table, order_by_column='ID', data_month_column='data_month', overwrite=true) %}
    {# 
    Macro to export data from a table/view to S3 using COPY INTO with dynamic timestamp from data_month field
    
    Parameters:
    - stage_name: Full stage name (e.g., 'dev_data_ingress.finance.s3_test_finance_automation_output')
    - file_prefix: File name prefix (e.g., 'partner_finance_mapped')
    - source_table: Full table/view name to export from
    - order_by_column: Column to order by (default: 'ID')
    - data_month_column: Column name containing the data_month value (default: 'data_month')
    - overwrite: Whether to overwrite existing files (default: true)
    
    Output filename format: {file_prefix}_YYYYMM.csv
    Example: partner_finance_mapped_202512.csv (based on actual data_month in the table)
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
    
    {# Remove existing files matching the pattern if overwrite is true #}
    {% if overwrite %}
        {# Use pattern matching to remove any files with the same prefix and month #}
        {% set remove_pattern = file_prefix ~ '_' ~ month_str ~ '.*' %}
        {% set remove_sql %}
        REMOVE @{{ stage_name }}/
        PATTERN = '{{ remove_pattern }}'
        {% endset %}
        {% do run_query(remove_sql) %}
        {{ log("Removed existing files matching pattern: " ~ remove_pattern, info=True) }}
    {% endif %}
    
    {# Export to S3 #}
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
                   NULL_IF = ('NULL', 'null', ''))
    SINGLE = TRUE
    OVERWRITE = TRUE
    ;
    {% endset %}
    
    {% do run_query(export_sql) %}
    {{ log("Data exported to S3: " ~ file_name ~ " from " ~ source_table ~ " (data_month: " ~ month_str ~ ")", info=True) }}
{% endmacro %}

