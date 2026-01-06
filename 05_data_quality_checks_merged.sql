-- Data Quality Checks for Merged View
-- View: dataeng_stage.public.view_partner_finance_mapped
-- Note: View uses LEFT JOIN with revenue aggregation as master table
-- All revenue records are included, mapping fields are optional (may be NULL)

-- ============================================================================
-- 1. REVENUE COVERAGE ANALYSIS (Primary Focus)
-- ============================================================================
WITH revenue_stats AS (
    SELECT 
        COUNT(DISTINCT r.ID) AS total_revenue_records,
        COUNT(DISTINCT CASE WHEN m.ID IS NOT NULL THEN r.ID END) AS mapped_revenue_records,
        COUNT(DISTINCT CASE WHEN m.ID IS NULL THEN r.ID END) AS unmapped_revenue_records
    FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
        ON r.ID = m.ID
    WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
),
mapping_stats AS (
    SELECT 
        COUNT(DISTINCT m.ID) AS total_mapping_records,
        COUNT(DISTINCT CASE WHEN r.ID IS NOT NULL THEN m.ID END) AS used_mapping_records,
        COUNT(DISTINCT CASE WHEN r.ID IS NULL THEN m.ID END) AS unused_mapping_records
    FROM dataeng_stage.public.mapping_template_raw_CURSOR m
    LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
        ON m.ID = r.ID
        AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
)
SELECT 
    'REVENUE COVERAGE SUMMARY' AS report_section,
    rs.total_revenue_records,
    rs.mapped_revenue_records,
    rs.unmapped_revenue_records,
    ROUND(rs.mapped_revenue_records * 100.0 / NULLIF(rs.total_revenue_records, 0), 2) AS revenue_coverage_rate_pct,
    ms.total_mapping_records,
    ms.used_mapping_records,
    ms.unused_mapping_records,
    ROUND(ms.used_mapping_records * 100.0 / NULLIF(ms.total_mapping_records, 0), 2) AS mapping_usage_rate_pct,
    CASE 
        WHEN rs.mapped_revenue_records * 100.0 / NULLIF(rs.total_revenue_records, 0) >= 95 THEN 'PASS'
        WHEN rs.mapped_revenue_records * 100.0 / NULLIF(rs.total_revenue_records, 0) >= 80 THEN 'WARNING'
        ELSE 'FAIL'
    END AS revenue_coverage_status
FROM revenue_stats rs
CROSS JOIN mapping_stats ms;

-- ============================================================================
-- 2. UNMAPPED REVENUE RECORDS (Primary Concern)
-- ============================================================================
-- These revenue records appear in the view but have NULL mapping fields
-- This is the main data quality concern - revenue without Oracle mapping
SELECT 
    'UNMAPPED REVENUE RECORDS' AS report_section,
    r.*,
    'No matching mapping record found - mapping fields will be NULL in view' AS unmapped_reason
FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
    ON r.ID = m.ID
WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
    AND m.ID IS NULL
ORDER BY r.ID;

-- Count of unmapped revenue records
SELECT 
    COUNT(*) AS unmapped_revenue_count,
    COUNT(DISTINCT r.ID) AS unmapped_revenue_ids
    -- Add revenue amount aggregation if available
    -- SUM(CASE WHEN revenue_amount IS NOT NULL THEN revenue_amount ELSE 0 END) AS unmapped_revenue_amount
FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
    ON r.ID = m.ID
WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
    AND m.ID IS NULL;

-- ============================================================================
-- 3. UNUSED MAPPING RECORDS (Secondary - Informational)
-- ============================================================================
-- These mappings exist but have no revenue for the prior month
-- Not a data quality issue, but useful for understanding mapping coverage
SELECT 
    'UNUSED MAPPING RECORDS' AS report_section,
    m.*,
    'No matching revenue record found for prior month' AS unused_reason
FROM dataeng_stage.public.mapping_template_raw_CURSOR m
LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    ON m.ID = r.ID
    AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
WHERE r.ID IS NULL
ORDER BY m.ID;

-- Count of unused mapping records
SELECT 
    COUNT(*) AS unused_mapping_count
FROM dataeng_stage.public.mapping_template_raw_CURSOR m
LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    ON m.ID = r.ID
    AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
WHERE r.ID IS NULL;
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
-- Note: NULL mapping fields are EXPECTED for unmapped revenue records
-- This is not a data quality failure, but indicates revenue without Oracle mapping
SELECT 
    'DATA COMPLETENESS CHECK' AS report_section,
    COUNT(*) AS total_revenue_records,
    COUNT(ID) AS non_null_ids,
    COUNT(Oracle_Customer_Name) AS non_null_customer_names,
    COUNT(Oracle_GL_Account) AS non_null_gl_accounts,
    COUNT(CASE WHEN data_month IS NULL THEN 1 END) AS null_data_months,
    COUNT(*) - COUNT(ID) AS null_ids,
    COUNT(*) - COUNT(Oracle_Customer_Name) AS null_customer_names,
    COUNT(*) - COUNT(Oracle_GL_Account) AS null_gl_accounts,
    ROUND((COUNT(*) - COUNT(Oracle_Customer_Name)) * 100.0 / NULLIF(COUNT(*), 0), 2) AS pct_revenue_without_mapping
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
-- Note: Records with NULL mapping fields represent unmapped revenue
SELECT 
    'REVENUE SUMMARY BY MAPPING' AS report_section,
    COALESCE(Oracle_Customer_Name, 'UNMAPPED') AS Oracle_Customer_Name,
    COALESCE(Oracle_Invoice_Group, 'UNMAPPED') AS Oracle_Invoice_Group,
    COALESCE(Oracle_GL_Account, 'UNMAPPED') AS Oracle_GL_Account,
    COUNT(DISTINCT ID) AS unique_ids,
    COUNT(*) AS total_records,
    -- Add revenue aggregation if available in the view
    -- SUM(revenue_amount) AS total_revenue
FROM dataeng_stage.public.view_partner_finance_mapped
GROUP BY Oracle_Customer_Name, Oracle_Invoice_Group, Oracle_GL_Account
ORDER BY total_records DESC;

-- Summary of unmapped revenue (revenue without mapping)
SELECT 
    'UNMAPPED REVENUE SUMMARY' AS report_section,
    COUNT(DISTINCT ID) AS unique_unmapped_ids,
    COUNT(*) AS total_unmapped_records
FROM dataeng_stage.public.view_partner_finance_mapped
WHERE Oracle_Customer_Name IS NULL 
   OR Oracle_GL_Account IS NULL;

-- ============================================================================
-- 7. COMPREHENSIVE DATA QUALITY REPORT
-- ============================================================================
WITH quality_metrics AS (
    SELECT 
        (SELECT COUNT(*) FROM dataeng_stage.public.view_partner_finance_mapped) AS total_revenue_records,
        (SELECT COUNT(DISTINCT ID) FROM dataeng_stage.public.view_partner_finance_mapped) AS unique_revenue_ids,
        (SELECT COUNT(DISTINCT r.ID) 
         FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
         WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))) AS total_revenue_in_source,
        (SELECT COUNT(DISTINCT r.ID) 
         FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
         LEFT JOIN dataeng_stage.public.mapping_template_raw_CURSOR m
             ON r.ID = m.ID
         WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
           AND m.ID IS NOT NULL) AS mapped_revenue_count,
        (SELECT COUNT(*) 
         FROM dataeng_stage.public.view_partner_finance_mapped
         WHERE Oracle_GL_Account IS NULL) AS unmapped_revenue_count,
        (SELECT COUNT(*) 
         FROM dataeng_stage.public.view_partner_finance_mapped
         WHERE Oracle_Customer_Name IS NULL) AS unmapped_customer_names,
        (SELECT COUNT(*) FROM dataeng_stage.public.mapping_template_raw_CURSOR) AS total_mapping_records
)
SELECT 
    'COMPREHENSIVE DATA QUALITY REPORT' AS report_section,
    total_revenue_records,
    unique_revenue_ids,
    CASE 
        WHEN total_revenue_records = unique_revenue_ids THEN 'PASS'
        ELSE 'FAIL - Duplicate revenue IDs found'
    END AS uniqueness_status,
    total_revenue_in_source,
    mapped_revenue_count,
    unmapped_revenue_count,
    ROUND(mapped_revenue_count * 100.0 / NULLIF(total_revenue_in_source, 0), 2) AS revenue_coverage_rate_pct,
    ROUND(unmapped_revenue_count * 100.0 / NULLIF(total_revenue_records, 0), 2) AS unmapped_revenue_pct,
    total_mapping_records,
    CASE 
        WHEN total_revenue_records = unique_revenue_ids 
         AND mapped_revenue_count * 100.0 / NULLIF(total_revenue_in_source, 0) >= 80
        THEN 'PASS - Data quality acceptable'
        WHEN mapped_revenue_count * 100.0 / NULLIF(total_revenue_in_source, 0) >= 50
        THEN 'WARNING - Low mapping coverage'
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

