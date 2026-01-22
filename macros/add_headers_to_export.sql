{% macro add_headers_to_export(
    stage_name,
    file_name,
    new_file_name,
    header_row=None,
    file_format_name='dev_data_ingress.finance.csv_format',
    num_columns=43
) %}
    {#
    Macro to add headers to an existing CSV file in S3 stage
    
    Parameters:
    - stage_name: Full stage name (e.g., 'dev_data_ingress.finance.s3_test_finance_automation_output')
    - file_name: Name of the existing file without headers (e.g., 'partner_finance_mapped_202509.csv')
    - new_file_name: Name of the new file with headers (e.g., 'partner_finance_mapped_202509_with_headers.csv')
    - header_row: Optional comma-separated header row. If not provided, uses default finance revenue headers
    - file_format_name: Name of the file format to use (default: 'dev_data_ingress.finance.csv_format')
    - num_columns: Number of columns in the file (default: 43)
    
    Usage:
    {{ add_headers_to_export(
        'dev_data_ingress.finance.s3_test_finance_automation_output',
        'partner_finance_mapped_202509.csv',
        'partner_finance_mapped_202509_with_headers.csv'
    ) }}
    #}
    
    {# Default header row if not provided #}
    {% if header_row is none %}
        {% set header_row = 'ORACLE_CUSTOMER_NAME,ORACLE_CUSTOMER_NAME_ID,ORACLE_INVOICE_GROUP,ORACLE_INVOICE_NAME,ORACLE_GL_ACCOUNT,ORACLE_MAPPED_RECORD_FLAG,ID_MAPPED_RECORD_FLAG,ID,DATA_MONTH,BUSINESS_UNIT_NAME,BUSINESS_UNIT_DETAIL_NAME,PRODUCT_LINE_ID,PRODUCT_LINE_NAME,PARENT_PARTNER_ID,PARENT_PARTNER_NAME,PARTNER_ID,PARTNER_NAME,DRID,CONTRACT_ID,CONTRACT_NAME,GAM_ADVERTISER_LEVEL1,GAM_ADVERTISER_LEVEL2,NETWORK_NAME_ID,NETWORK_NAME,NETWORK_CLASSIFICATION_NAME,NETWORK_CLASSIFICATION_SUBTYPE,NETWORK_FEED_ID,MARKUP_PERCENT,NET_PERCENT,REVSHARE_PERCENT,BAD_DEBT_PERCENT,MANAGEMENT_PERCENT,IMPRESSIONS,CLICKS,NETWORK_GROSS_REVENUE,NETWORK_REVSHARE,S1_GROSS_REVENUE,MANAGMENT_FEE,PARTNER_REVSHARE,HOLDBACK_REVENUE,PARTNER_GROSS_REVENUE,COMMISSION_AMOUNT,AGGREGATION_SOURCE' %}
    {% endif %}
    
    {# Step 1: Create or replace file format #}
    {% set create_format_sql %}
    CREATE OR REPLACE FILE FORMAT {{ file_format_name }}
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '\x22'
        NULL_IF = ('NULL', 'null', '')
    {% endset %}
    
    {% do run_query(create_format_sql) %}
    {{ log("File format created/verified: " ~ file_format_name, info=True) }}
    
    {# Step 2: Create header table #}
    {% set create_header_sql %}
    CREATE OR REPLACE TEMPORARY TABLE temp_header AS
    SELECT '{{ header_row }}' AS header_line;
    {% endset %}
    
    {% do run_query(create_header_sql) %}
    {{ log("Header table created", info=True) }}
    
    {# Step 3: Create temp table structure for data (43 columns as STRING) #}
    {% set col_defs = [] %}
    {% for i in range(1, num_columns + 1) %}
        {% set _ = col_defs.append("col" ~ i ~ " STRING") %}
    {% endfor %}
    
    {% set create_data_table_sql %}
    CREATE OR REPLACE TEMPORARY TABLE temp_export_data (
        {{ col_defs | join(', ') }}
    );
    {% endset %}
    
    {% do run_query(create_data_table_sql) %}
    {{ log("Temp data table created with " ~ num_columns ~ " columns", info=True) }}
    
    {# Step 4: Load data from existing file #}
    {% set copy_into_sql %}
    COPY INTO temp_export_data
    FROM @{{ stage_name }}/{{ file_name }}
    FILE_FORMAT = (FORMAT_NAME = '{{ file_format_name }}');
    {% endset %}
    
    {% do run_query(copy_into_sql) %}
    {{ log("Data loaded from file: " ~ file_name, info=True) }}
    
    {# Step 5: Create header split table #}
    {% set split_parts = [] %}
    {% for i in range(1, num_columns + 1) %}
        {% set _ = split_parts.append("SPLIT_PART(header_line, ',', " ~ i ~ ") AS col" ~ i) %}
    {% endfor %}
    
    {% set create_header_split_sql %}
    CREATE OR REPLACE TEMPORARY TABLE temp_header_split AS
    SELECT {{ split_parts | join(', ') }}
    FROM temp_header;
    {% endset %}
    
    {% do run_query(create_header_split_sql) %}
    {{ log("Header split table created", info=True) }}
    
    {# Step 6: Export header + data to new file #}
    {% set export_sql %}
    COPY INTO @{{ stage_name }}/{{ new_file_name }}
    FROM (SELECT * FROM temp_header_split UNION ALL SELECT * FROM temp_export_data)
    FILE_FORMAT = (FORMAT_NAME = '{{ file_format_name }}' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
    SINGLE = TRUE OVERWRITE = TRUE;
    {% endset %}
    
    {% do run_query(export_sql) %}
    {{ log("Headers added successfully. New file: " ~ new_file_name, info=True) }}
    
{% endmacro %}

