{{
    config(
        materialized='table',
        schema='public',
        database='dataeng_stage',
        tags=['finance', 'revenue', 'mapping', 's3_import'],
        pre_hook=[
            "{{ truncate_mapping_table() }}",
            "{{ load_from_s3_pattern(var('s3_mapping_stage', 'dev_data_ingress.finance.s3_test_finance_automation_input'), var('s3_mapping_file_pattern', '.*mapping.*\\.csv'), this) }}"
        ],
        post_hook=[
            "{{ validate_import() }}"
        ]
    )
}}

-- DBT Model: Import CSV Mapping File from S3 Bucket
-- This model loads the mapping template from S3 into Snowflake
-- Replaces manual execution of 02_import_from_s3.sql
-- 
-- The actual data loading happens in the pre-hook via the load_from_s3_pattern macro
-- This SELECT statement defines the table structure

SELECT
    ID::VARCHAR,
    Oracle_Customer_Name::VARCHAR,
    Oracle_Customer_Name_ID::VARCHAR,
    Oracle_Invoice_Group::VARCHAR,
    Oracle_Invoice_Name::VARCHAR,
    Oracle_GL_Account::VARCHAR
FROM {{ this }}

