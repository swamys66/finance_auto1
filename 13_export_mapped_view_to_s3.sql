-- Export Mapped View to S3 Bucket
-- Exports DEV_DATA_INGRESS.DBT_SSWAMYNATHAN_FINANCE._2_JOIN_REVENUE_WITH_MAPPING to S3
-- 
-- Note: HEADER parameter is NOT supported in FILE_FORMAT for COPY INTO exports
-- Removed HEADER = TRUE to fix the "invalid parameter header" error

COPY INTO @dev_data_ingress.finance.s3_test_finance_automation_output/partner_finance_mapped.csv
FROM (
    SELECT * 
    FROM DEV_DATA_INGRESS.DBT_SSWAMYNATHAN_FINANCE._2_JOIN_REVENUE_WITH_MAPPING
    ORDER BY ID
)
FILE_FORMAT = (TYPE = 'CSV' 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
               NULL_IF = ('NULL', 'null', ''))
SINGLE = TRUE
OVERWRITE = TRUE;

-- ============================================================================
-- OPTION: If you need headers in the CSV file, use this approach instead:
-- ============================================================================
/*
COPY INTO @dev_data_ingress.finance.s3_test_finance_automation_output/partner_finance_mapped.csv
FROM (
    -- First row: Column headers
    SELECT 
        'ID' AS col1, 'Oracle_Customer_Name' AS col2, 'Oracle_Customer_Name_ID' AS col3,
        'Oracle_Invoice_Group' AS col4, 'Oracle_Invoice_Name' AS col5, 'Oracle_GL_Account' AS col6,
        'Oracle_mapped_record_flag' AS col7, 'ID_mapped_record_flag' AS col8
        -- Add all other columns from your view
    WHERE 1=0
    
    UNION ALL
    
    -- Data rows
    SELECT * 
    FROM DEV_DATA_INGRESS.DBT_SSWAMYNATHAN_FINANCE._2_JOIN_REVENUE_WITH_MAPPING
    ORDER BY ID
)
FILE_FORMAT = (TYPE = 'CSV' 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
               NULL_IF = ('NULL', 'null', ''))
SINGLE = TRUE
OVERWRITE = TRUE;
*/

