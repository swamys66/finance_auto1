
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
Macro to export a DBT table/view to S3 with headers
Supports SnowflakeRelation objects (this) or string table names
#}

{# --- Extract database, schema, table --- #}
{% if source_table is string %}
    {% set table_parts = source_table.split('.') %}
    {% set database_name = table_parts[0] %}
    {% set schema_name = table_parts[1] %}
    {% set table_name_only = table_parts[2] %}
{% else %}
    {% set database_name = source_table.database %}
    {% set schema_name = source_table.schema %}
    {% set table_name_only = source_table.identifier %}
{% endif %}

{# --- Get data_month for filename --- #}
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
    {% set month_str = 'YYYYMM' %}
    {% set file_name = file_prefix ~ '_YYYYMM.csv' %}
{% endif %}

{# --- Get column names using DBT adapter --- #}
{% set relation = adapter.get_relation(database_name, schema_name, table_name_only) %}
{% set cols_info = adapter.get_columns_in_relation(relation) %}

{% set col_array = [] %}
{% for col in cols_info %}
    {% set _ = col_array.append(col.name) %}
{% endfor %}

{% if col_array | length == 0 %}
    {{ log("WARNING: Could not detect columns for " ~ source_table ~ ", exporting without headers", info=True) }}
    {% set has_headers = false %}
{% else %}
    {% set has_headers = true %}
    {{ log("Detected columns: " ~ col_array | join(','), info=True) }}
{% endif %}

{# --- Build header row (string literals) --- #}
{% if has_headers %}
    {% set header_select_parts = [] %}
    {% for col in col_array %}
        {% set _ = header_select_parts.append("'" ~ col | upper ~ "'::VARCHAR") %}
    {% endfor %}
    {% set header_select = header_select_parts | join(',') %}

    {# --- Build data select with double quotes for safety --- #}
    {% set data_select_parts = [] %}
    {% for col in col_array %}
        {% set _ = data_select_parts.append('"' ~ col ~ '"::VARCHAR') %}
    {% endfor %}
    {% set data_select = data_select_parts | join(',') %}
{% endif %}

{# --- Remove existing file if overwrite --- #}
{% if overwrite %}
    {% set remove_sql %}
    REMOVE @{{ stage_name }}/{{ file_name }};
    {% endset %}
    {% do run_query(remove_sql) %}
    {{ log("Removed existing file: " ~ file_name, info=True) }}
{% endif %}

{# --- Export to S3 --- #}
{% if has_headers %}
    {% set export_sql %}
    COPY INTO @{{ stage_name }}/{{ file_name }}
    FROM (
        -- Header row
        SELECT {{ header_select }}
        FROM (SELECT 1) AS t

        UNION ALL

        -- Data rows
        SELECT {{ data_select }}
        FROM (
            SELECT {{ data_select }}
            FROM {{ source_table }}
            {% if order_by_column %}
            ORDER BY "{{ order_by_column }}"
            {% endif %}
        )
    )
    FILE_FORMAT = (TYPE = 'CSV' 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                   NULL_IF = ('NULL', 'null', '')
                   ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                   ESCAPE_UNENCLOSED_FIELD = '\\'
                   REPLACE_INVALID_CHARACTERS = TRUE)
    SINGLE = TRUE
    OVERWRITE = TRUE
    HEADER = TRUE;
    {% endset %}
{% else %}
    {% set export_sql %}
    COPY INTO @{{ stage_name }}/{{ file_name }}
    FROM (
        SELECT *
        FROM {{ source_table }}
        {% if order_by_column %}
        ORDER BY "{{ order_by_column }}"
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
    HEADER = TRUE;
    {% endset %}
{% endif %}

{% do run_query(export_sql) %}
{{ log("Exported data to S3: " ~ file_name ~ " from " ~ source_table ~ " (data_month: " ~ month_str ~ ")" ~ (" with headers" if has_headers else " without headers"), info=True) }}
{% endmacro %}

