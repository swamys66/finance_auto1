-- DBT Macro: export_to_s3
-- Standalone macro file for exporting data to S3 using COPY INTO
-- 
-- This macro:
-- 1. Exports data from a table/view to S3 stage
-- 2. Supports single file or multiple files
-- 3. Allows custom file naming
--
-- Usage in dbt models:
--   post_hook: ["{{ export_to_s3('stage_name', 'file_name', 'table_name') }}"]
--
-- Or call directly:
--   {{ export_to_s3('dev_data_ingress.finance.s3_test_finance_automation_output', 'partner_finance_mapped.csv', 'dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping') }}

{% macro export_to_s3(stage_name, file_name, source_table, order_by_column='ID', single_file=true, overwrite=true) %}
    {# 
    Macro to export data from a table/view to S3 using COPY INTO
    
    Parameters:
    - stage_name: Full stage name (e.g., 'dev_data_ingress.finance.s3_test_finance_automation_output')
    - file_name: Output file name (e.g., 'partner_finance_mapped.csv')
    - source_table: Full table/view name to export from
    - order_by_column: Column to order by (default: 'ID')
    - single_file: Whether to create a single file (default: true)
    - overwrite: Whether to overwrite existing files (default: true)
    #}
    
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
    {% if single_file %}
    SINGLE = TRUE
    {% else %}
    SINGLE = FALSE
    {% endif %}
    {% if overwrite %}
    OVERWRITE = TRUE
    {% endif %}
    ;
    {% endset %}
    
    {% do run_query(export_sql) %}
    {{ log("Data exported to S3: " ~ file_name ~ " from " ~ source_table, info=True) }}
{% endmacro %}

