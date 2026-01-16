-- DBT Macro: load_from_s3_pattern
-- Standalone macro file for loading CSV data from S3 using pattern matching
-- 
-- This macro:
-- 1. Creates the table if it doesn't exist
-- 2. Executes COPY INTO from S3 stage using pattern matching
--
-- Usage in dbt models:
--   pre_hook: ["{{ load_from_s3_pattern('stage_name', 'pattern', 'table_name') }}"]
--
-- Or call directly:
--   {{ load_from_s3_pattern('dev_data_ingress.finance.s3_test_finance_automation_input', '.*mapping.*\\.csv', 'dev_data_ingress.finance.mapping_template_raw_cursor') }}

{% macro load_from_s3_pattern(stage_name, file_pattern, table_name) %}
    {# 
    Macro to load data from S3 using pattern matching
    Creates table if it doesn't exist, then executes COPY INTO
    
    Parameters:
    - stage_name: Full stage name (e.g., 'dev_data_ingress.finance.s3_test_finance_automation_input')
    - file_pattern: Pattern to match files (e.g., '.*mapping.*\\.csv')
    - table_name: Full table name (e.g., 'dev_data_ingress.finance.mapping_template_raw_cursor')
    #}
    
    {# Step 1: Create table if it doesn't exist #}
    {# Handle table_name - could be string or Relation object #}
    {% if table_name is string %}
        {% set table_name_str = table_name %}
    {% else %}
        {# If it's a Relation object, get the fully qualified name #}
        {% set table_name_str = table_name.include(database=true, schema=true) %}
    {% endif %}
    
    {% set create_table_sql %}
    CREATE TABLE IF NOT EXISTS {{ table_name_str }} (
        ID VARCHAR,
        Oracle_Customer_Name VARCHAR,
        Oracle_Customer_Name_ID VARCHAR,
        Oracle_Invoice_Group VARCHAR,
        Oracle_Invoice_Name VARCHAR,
        Oracle_GL_Account VARCHAR
    );
    {% endset %}
    
    {% do run_query(create_table_sql) %}
    {{ log("Table created/verified: " ~ table_name_str, info=True) }}
    
    {# Step 2: Execute COPY INTO #}
    {% set copy_sql %}
    COPY INTO {{ table_name_str }}
    (
        ID,
        Oracle_Customer_Name,
        Oracle_Customer_Name_ID,
        Oracle_Invoice_Group,
        Oracle_Invoice_Name,
        Oracle_GL_Account
    )
    FROM @{{ stage_name }}/
    FILE_FORMAT = (TYPE = 'CSV' 
                   SKIP_HEADER = 1 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                   ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                   NULL_IF = ('NULL', 'null', ''))
    PATTERN = '{{ file_pattern }}'
    ON_ERROR = 'ABORT_STATEMENT'
    PURGE = FALSE;
    {% endset %}
    
    {% do run_query(copy_sql) %}
    {{ log("Data loaded from S3 pattern: " ~ file_pattern ~ " into " ~ table_name_str, info=True) }}
{% endmacro %}

