{{
    config(
        materialized='view',
        schema='finance',
        database='dev_data_ingress',
        tags=['finance', 'revenue', 'export', 'headers'],
        description='Adds headers to an exported CSV file in S3 stage',
        post_hook=[
            "{{ add_headers_to_export(
                'dev_data_ingress.finance.s3_test_finance_automation_output',
                'partner_finance_mapped_202509.csv',
                'partner_finance_mapped_202509_with_headers.csv'
            ) }}"
        ]
    )
}}

-- ============================================================================
-- DBT Model: Add Headers to Exported CSV File
-- ============================================================================
-- This model adds headers to an existing CSV file in S3 stage using a dbt macro.
-- 
-- The actual work is done in the post-hook via the add_headers_to_export macro.
-- This model serves as a placeholder and can be used to track when headers
-- were added to exports.
--
-- Usage:
--   1. Update the post_hook parameters with your actual file names
--   2. Run: dbt run --select 20_dbt_add_headers_to_export
--   3. The macro will:
--      - Create/verify the CSV file format
--      - Load data from the existing file (without headers)
--      - Create header row and split it into columns
--      - Export header + data to a new file (with headers)
-- ============================================================================

SELECT 
    'Headers will be added via post-hook' AS status,
    'partner_finance_mapped_202509.csv' AS source_file_name,
    'partner_finance_mapped_202509_with_headers.csv' AS target_file_name,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    'dev_data_ingress.finance.s3_test_finance_automation_output' AS stage_name;

