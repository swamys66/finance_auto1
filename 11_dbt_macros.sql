-- DBT Macros for Finance Revenue S3 Import
-- These macros support the dbt model for importing from S3
-- Note: Stage creation is a one-time setup and not included in these macros
-- The stage dev_data_ingress.finance.s3_test_finance_automation_input must already exist

{% macro truncate_mapping_table() %}
    {# Truncate the mapping table before loading new data #}
    
    {% set truncate_sql %}
    TRUNCATE TABLE IF EXISTS {{ this }}
    {% endset %}
    
    {% do run_query(truncate_sql) %}
    {{ log("Mapping table truncated", info=True) }}
{% endmacro %}


{% macro validate_import() %}
    {# Validate that the import was successful #}
    
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
            {{ exceptions.raise_compiler_error("Import validation failed: No rows loaded") }}
        {% elif row_count != unique_count %}
            {{ log("WARNING: Duplicate IDs found in imported data", info=True) }}
        {% elif null_count > 0 %}
            {{ log("WARNING: NULL IDs found in imported data", info=True) }}
        {% else %}
            {{ log("Import validation passed: " ~ row_count ~ " rows loaded", info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro load_from_s3_pattern(stage_name, file_pattern, table_name) %}
    {# Generic macro to load data from S3 using pattern matching #}
    {# Creates table if it doesn't exist, then executes COPY INTO #}
    
    {# Step 1: Create table if it doesn't exist #}
    {# Handle table_name - could be string or Relation object #}
    {% if table_name is string %}
        {% set table_name_rendered = table_name %}
    {% else %}
        {# If it's a Relation object, render it with database and schema #}
        {% set table_name_rendered = adapter.quote(table_name.database) ~ '.' ~ adapter.quote(table_name.schema) ~ '.' ~ adapter.quote(table_name.identifier) %}
    {% endif %}
    
    {% set create_table_sql %}
    CREATE TABLE IF NOT EXISTS {{ table_name_rendered }} (
        ID VARCHAR,
        Oracle_Customer_Name VARCHAR,
        Oracle_Customer_Name_ID VARCHAR,
        Oracle_Invoice_Group VARCHAR,
        Oracle_Invoice_Name VARCHAR,
        Oracle_GL_Account VARCHAR
    );
    {% endset %}
    
    {% do run_query(create_table_sql) %}
    {{ log("Table created/verified: " ~ table_name_rendered, info=True) }}
    
    {# Step 2: Execute COPY INTO #}
    {% set copy_sql %}
    COPY INTO {{ table_name_rendered }}
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
    {{ log("Data loaded from S3 pattern: " ~ file_pattern ~ " into " ~ table_name_rendered, info=True) }}
{% endmacro %}


{% macro load_from_s3_file(stage_name, file_name, table_name) %}
    {# Generic macro to load data from S3 using specific file name #}
    {# Creates table if it doesn't exist, then executes COPY INTO #}
    
    {# Step 1: Create table if it doesn't exist #}
    {# Handle table_name - could be string or Relation object #}
    {% if table_name is string %}
        {% set table_name_rendered = table_name %}
    {% else %}
        {# If it's a Relation object, render it with database and schema #}
        {% set table_name_rendered = adapter.quote(table_name.database) ~ '.' ~ adapter.quote(table_name.schema) ~ '.' ~ adapter.quote(table_name.identifier) %}
    {% endif %}
    
    {% set create_table_sql %}
    CREATE TABLE IF NOT EXISTS {{ table_name_rendered }} (
        ID VARCHAR,
        Oracle_Customer_Name VARCHAR,
        Oracle_Customer_Name_ID VARCHAR,
        Oracle_Invoice_Group VARCHAR,
        Oracle_Invoice_Name VARCHAR,
        Oracle_GL_Account VARCHAR
    );
    {% endset %}
    
    {% do run_query(create_table_sql) %}
    {{ log("Table created/verified: " ~ table_name_rendered, info=True) }}
    
    {# Step 2: Execute COPY INTO #}
    {% set copy_sql %}
    COPY INTO {{ table_name_rendered }}
    (
        ID,
        Oracle_Customer_Name,
        Oracle_Customer_Name_ID,
        Oracle_Invoice_Group,
        Oracle_Invoice_Name,
        Oracle_GL_Account
    )
    FROM @{{ stage_name }}/{{ file_name }}
    FILE_FORMAT = (TYPE = 'CSV' 
                   SKIP_HEADER = 1 
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                   ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                   NULL_IF = ('NULL', 'null', ''))
    ON_ERROR = 'ABORT_STATEMENT'
    PURGE = FALSE;
    {% endset %}
    
    {% do run_query(copy_sql) %}
    {{ log("Data loaded from S3 file: " ~ file_name ~ " into " ~ table_name_rendered, info=True) }}
{% endmacro %}

