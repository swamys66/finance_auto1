{{
    config(
        materialized='table',
        schema='finance',
        database='dev_data_ingress',
        tags=['finance', 'revenue', 'mapping', 's3_import'],
        post_hook=[
            "{{ validate_import() }}"
        ]
    )
}}

-- DBT Model: Import CSV Mapping File from S3 Bucket (Simple Version)
-- Alternative approach: Use this if the pre-hook COPY INTO approach doesn't work
-- 
-- This version requires running COPY INTO manually first (using 02_import_from_s3.sql),
-- then this model can be used for data quality checks and transformations
-- 
-- Note: Stage creation is a one-time setup and not part of this dbt process

SELECT
    ID::VARCHAR,
    Oracle_Customer_Name::VARCHAR,
    Oracle_Customer_Name_ID::VARCHAR,
    Oracle_Invoice_Group::VARCHAR,
    Oracle_Invoice_Name::VARCHAR,
    Oracle_GL_Account::VARCHAR
FROM {{ ref('mapping_template_raw_cursor') }}
WHERE ID IS NOT NULL

