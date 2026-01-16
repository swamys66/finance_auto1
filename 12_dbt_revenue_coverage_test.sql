-- DBT Model: Revenue Coverage Test Summary
-- This model provides a detailed summary of revenue mapping coverage
-- Can be used for reporting and monitoring
-- 
-- Note: This is a model (not a test) that shows the full summary regardless of pass/fail
-- For automated testing, use tests/test_revenue_coverage.sql
--
-- Usage:
--   dbt run --select 12_dbt_revenue_coverage_test

{{
    config(
        materialized='view',
        schema='finance',
        database='dev_data_ingress',
        tags=['finance', 'revenue', 'mapping', 'data_quality', 'test'],
        description='Revenue coverage summary showing mapping statistics and validation status'
    )
}}

WITH revenue_stats AS (
    SELECT 
        COUNT(DISTINCT r.ID) AS total_revenue_records,
        COUNT(DISTINCT CASE WHEN m.ID IS NOT NULL THEN r.ID END) AS mapped_revenue_records,
        COUNT(DISTINCT CASE WHEN m.ID IS NULL THEN r.ID END) AS unmapped_revenue_records
    FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    LEFT JOIN {{ ref('_1_import_from_s3') }} m
        ON r.ID = m.ID
    WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -4, CURRENT_DATE()))
),
mapping_stats AS (
    SELECT 
        COUNT(DISTINCT m.ID) AS total_unique_mapping_records,
        COUNT(*) AS total_mapping_records,
        COUNT(DISTINCT CASE WHEN r.ID IS NOT NULL THEN m.ID END) AS used_mapping_records,
        COUNT(DISTINCT CASE WHEN r.ID IS NULL THEN m.ID END) AS unused_mapping_records
    FROM {{ ref('_1_import_from_s3') }} m
    LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
        ON m.ID = r.ID
        AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -4, CURRENT_DATE()))
)
SELECT 
    'REVENUE COVERAGE SUMMARY' AS report_section,
    rs.total_revenue_records,
    rs.mapped_revenue_records,
    rs.unmapped_revenue_records,
    ROUND(rs.mapped_revenue_records * 100.0 / NULLIF(rs.total_revenue_records, 0), 2) AS revenue_coverage_rate_pct,
    ms.total_unique_mapping_records,
    ms.total_mapping_records,
    ms.used_mapping_records,
    ms.unused_mapping_records,
    ROUND(ms.used_mapping_records * 100.0 / NULLIF(ms.total_mapping_records, 0), 2) AS mapping_usage_rate_pct,
    CASE 
        WHEN rs.mapped_revenue_records * 100.0 / NULLIF(rs.total_revenue_records, 0) >= 95 THEN 'PASS'
        WHEN rs.mapped_revenue_records * 100.0 / NULLIF(rs.total_revenue_records, 0) >= 80 THEN 'WARNING'
        ELSE 'FAIL'
    END AS revenue_coverage_status,
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    '4 months prior' AS data_month_filter
FROM revenue_stats rs
CROSS JOIN mapping_stats ms

