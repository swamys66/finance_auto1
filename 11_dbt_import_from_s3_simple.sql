{{
    config(
        materialized='table',
        schema='finance',
        database='dev_data_ingress',
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

-- DBT Model: Import CSV Mapping File from S3 Bucket (Simple Version)
-- This version uses COPY INTO via dbt macros, same as the main version
-- 
-- Differences from main version:
-- - Filters out NULL IDs in the SELECT
-- - Simpler structure for basic use cases
-- 
-- The COPY INTO operation happens automatically in pre-hooks via load_from_s3_pattern macro
-- This replaces the need to manually run 02_import_from_s3.sql

SELECT
    ID::VARCHAR,
    Oracle_Customer_Name::VARCHAR,
    Oracle_Customer_Name_ID::VARCHAR,
    Oracle_Invoice_Group::VARCHAR,
    Oracle_Invoice_Name::VARCHAR,
    Oracle_GL_Account::VARCHAR
FROM {{ this }}
WHERE ID IS NOT NULL

