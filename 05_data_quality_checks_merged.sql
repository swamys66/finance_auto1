-- Data Quality Checks for Merged View
-- View: dataeng_stage.public.view_partner_finance_mapped

-- ============================================================================
-- 1. MAPPING SUCCESS ANALYSIS
-- ============================================================================
WITH mapping_stats AS (
    SELECT 
        COUNT(DISTINCT m.ID) AS total_mapping_records,
        COUNT(DISTINCT CASE WHEN r.ID IS NOT NULL THEN m.ID END) AS mapped_records,
        COUNT(DISTINCT CASE WHEN r.ID IS NULL THEN m.ID END) AS unmapped_mapping_records
    FROM dataeng_stage.public.mapping_template_raw_CURSOR m
    LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
        ON m.ID = r.ID
        AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
),
revenue_stats AS (
    SELECT 
        COUNT(DISTINCT r.ID) AS total_revenue_records,
        COUNT(DISTINCT CASE WHEN m.ID IS NOT NULL THEN r.ID END) AS mapped_revenue_records,
        COUNT(DISTINCT CASE WHEN m.ID IS NULL THEN r.ID END) AS unmapped_revenue_records
    FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
        ON r.ID = m.ID
    WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
)
SELECT 
    'MAPPING SUCCESS SUMMARY' AS report_section,
    ms.total_mapping_records,
    ms.mapped_records,
    ms.unmapped_mapping_records,
    ROUND(ms.mapped_records * 100.0 / NULLIF(ms.total_mapping_records, 0), 2) AS mapping_success_rate_pct,
    rs.total_revenue_records,
    rs.mapped_revenue_records,
    rs.unmapped_revenue_records,
    ROUND(rs.mapped_revenue_records * 100.0 / NULLIF(rs.total_revenue_records, 0), 2) AS revenue_coverage_rate_pct,
    CASE 
        WHEN ms.mapped_records * 100.0 / NULLIF(ms.total_mapping_records, 0) >= 95 THEN 'PASS'
        WHEN ms.mapped_records * 100.0 / NULLIF(ms.total_mapping_records, 0) >= 80 THEN 'WARNING'
        ELSE 'FAIL'
    END AS mapping_status
FROM mapping_stats ms
CROSS JOIN revenue_stats rs;

-- ============================================================================
-- 2. UNMAPPED MAPPING RECORDS
-- ============================================================================
SELECT 
    'UNMAPPED MAPPING RECORDS' AS report_section,
    m.*,
    'No matching revenue record found for prior month' AS unmapped_reason
FROM dataeng_stage.public.mapping_template_raw_CURSOR m
LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    ON m.ID = r.ID
    AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
WHERE r.ID IS NULL
ORDER BY m.ID;

-- Count of unmapped mapping records
SELECT 
    COUNT(*) AS unmapped_mapping_count
FROM dataeng_stage.public.mapping_template_raw_CURSOR m
LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    ON m.ID = r.ID
    AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
WHERE r.ID IS NULL;

-- ============================================================================
-- 3. UNMAPPED REVENUE RECORDS
-- ============================================================================
SELECT 
    'UNMAPPED REVENUE RECORDS' AS report_section,
    r.*,
    'No matching mapping record found' AS unmapped_reason
FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
    ON r.ID = m.ID
WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
    AND m.ID IS NULL
ORDER BY r.ID;

-- Count of unmapped revenue records
SELECT 
    COUNT(*) AS unmapped_revenue_count,
    SUM(CASE WHEN revenue_amount IS NOT NULL THEN revenue_amount ELSE 0 END) AS unmapped_revenue_amount
FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
    ON r.ID = m.ID
WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
    AND m.ID IS NULL;

-- ============================================================================
-- 4. DATA COMPLETENESS IN MERGED VIEW
-- ============================================================================
SELECT 
    'DATA COMPLETENESS CHECK' AS report_section,
    COUNT(*) AS total_mapped_records,
    COUNT(ID) AS non_null_ids,
    COUNT(Oracle_Customer_Name) AS non_null_customer_names,
    COUNT(Oracle_GL_Account) AS non_null_gl_accounts,
    COUNT(CASE WHEN data_month IS NULL THEN 1 END) AS null_data_months,
    COUNT(*) - COUNT(ID) AS null_ids,
    COUNT(*) - COUNT(Oracle_Customer_Name) AS null_customer_names,
    COUNT(*) - COUNT(Oracle_GL_Account) AS null_gl_accounts
FROM dataeng_stage.public.view_partner_finance_mapped;

-- ============================================================================
-- 5. DUPLICATE CHECK IN MERGED VIEW
-- ============================================================================
-- Check for duplicate ID mappings (should not happen if mapping table is unique)
SELECT 
    'DUPLICATE CHECK' AS report_section,
    ID,
    COUNT(*) AS occurrence_count
FROM dataeng_stage.public.view_partner_finance_mapped
GROUP BY ID
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;

-- ============================================================================
-- 6. REVENUE SUMMARY BY MAPPING FIELDS
-- ============================================================================
-- Summary of revenue by Oracle mapping fields
SELECT 
    'REVENUE SUMMARY BY MAPPING' AS report_section,
    Oracle_Customer_Name,
    Oracle_Invoice_Group,
    Oracle_GL_Account,
    COUNT(DISTINCT ID) AS unique_ids,
    COUNT(*) AS total_records,
    -- Add revenue aggregation if available in the view
    -- SUM(revenue_amount) AS total_revenue
FROM dataeng_stage.public.view_partner_finance_mapped
GROUP BY Oracle_Customer_Name, Oracle_Invoice_Group, Oracle_GL_Account
ORDER BY total_records DESC;

-- ============================================================================
-- 7. COMPREHENSIVE DATA QUALITY REPORT
-- ============================================================================
WITH quality_metrics AS (
    SELECT 
        (SELECT COUNT(*) FROM dataeng_stage.public.view_partner_finance_mapped) AS total_mapped_records,
        (SELECT COUNT(DISTINCT ID) FROM dataeng_stage.public.view_partner_finance_mapped) AS unique_mapped_ids,
        (SELECT COUNT(*) FROM dataeng_stage.public.mapping_template_raw_CURSOR) AS total_mapping_records,
        (SELECT COUNT(DISTINCT m.ID) 
         FROM dataeng_stage.public.mapping_template_raw_CURSOR m
         INNER JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
             ON m.ID = r.ID
         WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))) AS successfully_mapped_records,
        (SELECT COUNT(*) 
         FROM dataeng_stage.public.view_partner_finance_mapped
         WHERE Oracle_GL_Account IS NULL) AS null_gl_accounts,
        (SELECT COUNT(*) 
         FROM dataeng_stage.public.view_partner_finance_mapped
         WHERE Oracle_Customer_Name IS NULL) AS null_customer_names
)
SELECT 
    'COMPREHENSIVE DATA QUALITY REPORT' AS report_section,
    total_mapped_records,
    unique_mapped_ids,
    CASE 
        WHEN total_mapped_records = unique_mapped_ids THEN 'PASS'
        ELSE 'FAIL - Duplicates found'
    END AS uniqueness_status,
    total_mapping_records,
    successfully_mapped_records,
    total_mapping_records - successfully_mapped_records AS unmapped_count,
    ROUND(successfully_mapped_records * 100.0 / NULLIF(total_mapping_records, 0), 2) AS mapping_success_rate_pct,
    null_gl_accounts,
    null_customer_names,
    CASE 
        WHEN total_mapped_records = unique_mapped_ids 
         AND null_gl_accounts = 0 
         AND successfully_mapped_records * 100.0 / NULLIF(total_mapping_records, 0) >= 80
        THEN 'PASS - Data quality acceptable'
        ELSE 'FAIL - Data quality issues detected'
    END AS overall_status
FROM quality_metrics;

-- ============================================================================
-- 8. SAMPLE DATA PREVIEW
-- ============================================================================
SELECT 
    'SAMPLE MAPPED DATA' AS report_section,
    *
FROM dataeng_stage.public.view_partner_finance_mapped
LIMIT 10;

