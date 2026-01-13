-- DBT Macros for Finance Revenue S3 Import
-- These macros support the dbt model for importing from S3

{% macro create_s3_mapping_stage() %}
    {# Verify external stage exists (stage is pre-configured: dev_data_ingress.finance.s3_test_finance_automation_input) #}
    {# This macro verifies the stage exists rather than creating it #}
    
    {% set stage_name = var('s3_mapping_stage', 'dev_data_ingress.finance.s3_test_finance_automation_input') %}
    
    {% set verify_sql %}
    DESCRIBE STAGE {{ stage_name }};
    {% endset %}
    
    {% set results = run_query(verify_sql) %}
    
    {% if execute %}
        {{ log("S3 mapping stage verified: " ~ stage_name, info=True) }}
    {% else %}
        {{ exceptions.raise_compiler_error("S3 mapping stage not found: " ~ stage_name ~ ". Please ensure the stage exists.") }}
    {% endif %}
    
    {# Optional: Uncomment below to create stage if it doesn't exist (not needed if stage is pre-configured) #}
    {#
    {% set stage_sql %}
    CREATE STAGE IF NOT EXISTS {{ stage_name }}
        URL = '{{ var("s3_mapping_bucket_url", "s3://your-bucket-name/mapping-files/") }}'
        CREDENTIALS = (
            AWS_KEY_ID = '{{ var("aws_key_id", "your-aws-access-key-id") }}'
            AWS_SECRET_KEY = '{{ var("aws_secret_key", "your-aws-secret-access-key") }}'
        )
        FILE_FORMAT = (TYPE = 'CSV' 
                       SKIP_HEADER = 1 
                       FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                       ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                       NULL_IF = ('NULL', 'null', ''));
    {% endset %}
    
    {% do run_query(stage_sql) %}
    {{ log("S3 mapping stage created/verified", info=True) }}
    #}
{% endmacro %}


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
    
    {% set copy_sql %}
    COPY INTO {{ table_name }}
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
    {{ log("Data loaded from S3 pattern: " ~ file_pattern, info=True) }}
{% endmacro %}


{% macro load_from_s3_file(stage_name, file_name, table_name) %}
    {# Generic macro to load data from S3 using specific file name #}
    
    {% set copy_sql %}
    COPY INTO {{ table_name }}
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
    {{ log("Data loaded from S3 file: " ~ file_name, info=True) }}
{% endmacro %}

