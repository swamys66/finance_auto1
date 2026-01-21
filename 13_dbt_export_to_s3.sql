{{
    config(
        materialized='view',
        schema='finance',
        database='dev_data_ingress',
        tags=['finance', 'revenue', 'mapping', 'export', 's3'],
        description='Exports mapped finance revenue view to S3 bucket',
        post_hook=[
            "{{ export_to_s3('dev_data_ingress.finance.s3_test_finance_automation_output', 'partner_finance_mapped.csv', 'dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping', 'ID', true, true) }}"
        ]
    )
}}

-- DBT Model: Export Mapped Finance Revenue View to S3
-- This model exports the mapped view to S3 bucket
-- 
-- The actual export happens in the post-hook via the export_to_s3 macro
-- This SELECT statement is required for dbt but the export happens automatically

SELECT 
    'Export will be executed via post-hook' AS export_status,
    CURRENT_TIMESTAMP() AS export_timestamp,
    'dev_data_ingress.dbt_sswamynathan_finance._2_join_revenue_with_mapping' AS source_table,
    'dev_data_ingress.finance.s3_test_finance_automation_output/partner_finance_mapped.csv' AS target_file

