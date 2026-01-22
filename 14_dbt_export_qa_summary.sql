{{
    config(
        materialized='view',
        schema='finance',
        database='dev_data_ingress',
        tags=['finance', 'revenue', 'mapping', 'export', 'qa', 'validation'],
        description='QA summary for exported finance revenue mapped file - Validates export completeness and data integrity'
    )
}}

-- DBT Model: Export QA Summary
-- This model provides comprehensive QA validation for the exported S3 file
-- 
-- Validations:
-- - File existence check
-- - Row count comparison (source vs exported)
-- - Data integrity checks
-- - File metadata (size, last modified)
-- - Export status (PASS/WARNING/FAIL)

WITH source_stats AS (
    SELECT 
        COUNT(*) AS source_row_count,
        COUNT(DISTINCT ID) AS source_unique_ids,
        MIN(data_month) AS min_data_month,
        MAX(data_month) AS max_data_month,
        COUNT(DISTINCT data_month) AS distinct_data_months
    FROM {{ ref('_2_join_revenue_with_mapping') }}
),
export_file_list AS (
    SELECT 
        METADATA$FILENAME AS file_name,
        MAX(METADATA$FILE_ROW_NUMBER) AS exported_row_count_from_metadata,
        MAX(METADATA$FILE_LAST_MODIFIED) AS last_modified,
        MAX(METADATA$FILE_CONTENT_KEY) AS content_key,
        ROW_NUMBER() OVER (ORDER BY MAX(METADATA$FILE_LAST_MODIFIED) DESC) AS rn
    FROM @dev_data_ingress.finance.s3_test_finance_automation_output
    WHERE METADATA$FILENAME LIKE '%partner_finance_mapped%'
    GROUP BY METADATA$FILENAME
),
latest_export AS (
    SELECT *
    FROM export_file_list
    WHERE rn = 1
),
export_data_stats AS (
    SELECT 
        MAX(METADATA$FILE_ROW_NUMBER) AS exported_data_row_count,
        COUNT(DISTINCT CASE WHEN $1 IS NOT NULL THEN $1 END) AS exported_unique_ids  -- Assuming ID is first column, exclude NULLs
    FROM @dev_data_ingress.finance.s3_test_finance_automation_output
    WHERE METADATA$FILENAME = (SELECT file_name FROM latest_export)
),
qa_summary AS (
    SELECT 
        'EXPORT QA SUMMARY' AS report_section,
        ss.source_row_count,
        ed.exported_data_row_count,
        ss.source_unique_ids,
        ed.exported_unique_ids,
        le.file_name,
        le.last_modified,
        ss.min_data_month,
        ss.max_data_month,
        ss.distinct_data_months,
        -- Calculate differences
        ss.source_row_count - ed.exported_data_row_count AS row_count_difference,
        ROUND((ed.exported_data_row_count * 100.0 / NULLIF(ss.source_row_count, 0)), 2) AS export_completeness_pct,
        -- Validation status
        CASE 
            WHEN le.file_name IS NULL THEN 'FAIL - File not found'
            WHEN ed.exported_data_row_count = 0 THEN 'FAIL - File is empty'
            WHEN ss.source_row_count != ed.exported_data_row_count THEN 'WARNING - Row count mismatch'
            WHEN ss.source_unique_ids != ed.exported_unique_ids THEN 'WARNING - Unique ID count mismatch'
            ELSE 'PASS - Export validation successful'
        END AS export_status,
        -- Additional checks
        CASE 
            WHEN le.file_name IS NULL THEN 'File does not exist in S3 stage'
            WHEN ed.exported_data_row_count = 0 THEN 'Exported file contains no data'
            WHEN ss.source_row_count != ed.exported_data_row_count THEN 
                'Source has ' || ss.source_row_count || ' rows, exported has ' || ed.exported_data_row_count || ' rows'
            ELSE 'All validations passed'
        END AS validation_details,
        CURRENT_TIMESTAMP() AS qa_run_timestamp
    FROM source_stats ss
    CROSS JOIN export_data_stats ed
    CROSS JOIN latest_export le
)
SELECT * FROM qa_summary

