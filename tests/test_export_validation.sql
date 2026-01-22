-- DBT Test: Export File Validation
-- This test validates that the exported file exists and has correct row count
-- 
-- Test Logic:
-- - Checks if exported file exists in S3 stage
-- - Validates row count matches source table
-- - Returns rows if validation fails (test fails)
-- - Returns 0 rows if validation passes (test passes)
--
-- Usage:
--   dbt test --select test_export_validation
--   Or: dbt test -s test_export_validation

WITH source_stats AS (
    SELECT 
        COUNT(*) AS source_row_count,
        COUNT(DISTINCT ID) AS source_unique_ids
    FROM {{ ref('_2_join_revenue_with_mapping') }}
),
file_list AS (
    SELECT 
        METADATA$FILENAME AS file_name,
        MAX(METADATA$FILE_ROW_NUMBER) AS row_count,
        MAX(METADATA$FILE_LAST_MODIFIED) AS last_modified
    FROM @dev_data_ingress.finance.s3_test_finance_automation_output
    WHERE METADATA$FILENAME LIKE '%partner_finance_mapped%'
    GROUP BY METADATA$FILENAME
    ORDER BY MAX(METADATA$FILE_LAST_MODIFIED) DESC
    LIMIT 1
),
export_stats AS (
    SELECT 
        MAX(METADATA$FILE_ROW_NUMBER) AS exported_row_count,
        COUNT(DISTINCT CASE WHEN $1 IS NOT NULL THEN $1 END) AS exported_unique_ids  -- Assuming ID is first column, exclude NULLs
    FROM @dev_data_ingress.finance.s3_test_finance_automation_output
    WHERE METADATA$FILENAME = (SELECT file_name FROM file_list)
)
SELECT 
    'EXPORT VALIDATION FAILED' AS validation_status,
    ss.source_row_count,
    es.exported_row_count,
    ss.source_unique_ids,
    es.exported_unique_ids,
    fl.file_name,
    fl.last_modified,
    CASE 
        WHEN fl.file_name IS NULL THEN 'File not found in S3'
        WHEN ss.source_row_count != es.exported_row_count THEN 'Row count mismatch'
        WHEN es.exported_row_count = 0 THEN 'Exported file is empty'
        ELSE 'Unknown validation error'
    END AS failure_reason
FROM source_stats ss
CROSS JOIN export_stats es
CROSS JOIN file_list fl
WHERE 
    -- Test fails if any of these conditions are true
    fl.file_name IS NULL
    OR ss.source_row_count != es.exported_row_count
    OR es.exported_row_count = 0

