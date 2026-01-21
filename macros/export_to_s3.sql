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
--   post_hook: ["{{ export_to_s3('stage_name', 'file_prefix', 'table_name', 'order_column', months_back) }}"]
--
-- Example:
--   {{ export_to_s3('dev_data_ingress.finance.s3_test_finance_automation_output', 'partner_finance_mapped', 'dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping', 'ID', -4) }}

{% macro export_to_s3(stage_name, file_prefix, source_table, order_by_column='ID', months_back=-4, overwrite=true) %}
    {# 
    Macro to export data from a table/view to S3 using COPY INTO with dynamic timestamp
    
    Parameters:
    - stage_name: Full stage name (e.g., 'dev_data_ingress.finance.s3_test_finance_automation_output')
    - file_prefix: File name prefix (e.g., 'partner_finance_mapped')
    - source_table: Full table/view name to export from
    - order_by_column: Column to order by (default: 'ID')
    - months_back: Number of months back for timestamp (default: -4 for 4 months prior)
    - overwrite: Whether to overwrite existing files (default: true)
    
    Output filename format: {file_prefix}_YYYYMM.csv
    Example: partner_finance_mapped_202512.csv
    #}
    
    {# Generate dynamic filename with timestamp #}
    {% set timestamp_sql %}
    SELECT TO_CHAR(DATEADD(MONTH, {{ months_back }}, CURRENT_DATE()), 'YYYYMM') AS month_str
    {% endset %}
    
    {% set timestamp_result = run_query(timestamp_sql) %}
    {% if execute %}
        {% set month_str = timestamp_result.columns[0].values()[0] %}
        {% set file_name = file_prefix ~ '_' ~ month_str ~ '.csv' %}
    {% else %}
        {% set file_name = file_prefix ~ '_' ~ 'YYYYMM' ~ '.csv' %}
    {% endif %}
    
    {% set export_sql %}
    COPY INTO @{{ stage_name }}/{{ file_prefix }}_
        || TO_CHAR(DATEADD(MONTH, {{ months_back }}, CURRENT_DATE()), 'YYYYMM') 
        || '.csv'
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
    {% if overwrite %}
    OVERWRITE = TRUE
    {% endif %}
    ;
    {% endset %}
    
    {% do run_query(export_sql) %}
    {{ log("Data exported to S3: " ~ file_prefix ~ "_[timestamp].csv from " ~ source_table, info=True) }}
{% endmacro %}

