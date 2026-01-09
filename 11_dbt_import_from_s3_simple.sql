{{
    config(
        materialized='table',
        schema='public',
        database='dataeng_stage',
        tags=['finance', 'revenue', 'mapping', 's3_import'],
        pre_hook=[
            "{{ create_s3_mapping_stage() }}"
        ],
        post_hook=[
            "{{ validate_import() }}"
        ]
    )
}}

-- DBT Model: Import CSV Mapping File from S3 Bucket (Simple Version)
-- Alternative approach: Use this if the pre-hook COPY INTO approach doesn't work
-- 
-- This version requires running COPY INTO manually first, then this model
-- can be used for data quality checks and transformations

SELECT
    ID::VARCHAR,
    Oracle_Customer_Name::VARCHAR,
    Oracle_Customer_Name_ID::VARCHAR,
    Oracle_Invoice_Group::VARCHAR,
    Oracle_Invoice_Name::VARCHAR,
    Oracle_GL_Account::VARCHAR
FROM {{ ref('mapping_template_raw_cursor') }}
WHERE ID IS NOT NULL

