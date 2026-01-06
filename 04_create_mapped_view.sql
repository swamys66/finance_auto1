-- Create Merged View: view_partner_finance_mapped
-- Joins mapping table with revenue aggregation view for prior month

-- ============================================================================
-- Option 1: Dynamic Prior Month (Recommended for automation)
-- ============================================================================
CREATE OR REPLACE VIEW dataeng_stage.public.view_partner_finance_mapped AS
SELECT 
    -- Mapping fields from mapping_template_raw_CURSOR
    m.ID,
    m.Oracle_Customer_Name,
    m.Oracle_Customer_Name_ID,
    m.Oracle_Invoice_Group,
    m.Oracle_Invoice_Name,
    m.Oracle_GL_Account,
    
    -- All fields from revenue aggregation view
    r.*
    
FROM dataeng_stage.public.mapping_template_raw_CURSOR m
INNER JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    ON m.ID = r.ID
WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()));

-- ============================================================================
-- Option 2: Specific Month (Use for one-time runs or testing)
-- ============================================================================
-- Uncomment and modify the date below for specific month processing
/*
CREATE OR REPLACE VIEW dataeng_stage.public.view_partner_finance_mapped AS
SELECT 
    m.ID,
    m.Oracle_Customer_Name,
    m.Oracle_Customer_Name_ID,
    m.Oracle_Invoice_Group,
    m.Oracle_Invoice_Name,
    m.Oracle_GL_Account,
    r.*
FROM dataeng_stage.public.mapping_template_raw_CURSOR m
INNER JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    ON m.ID = r.ID
WHERE r.data_month = '2025-12-01';  -- Update with target month
*/

-- ============================================================================
-- Option 3: LEFT JOIN to include all mapping records (even without revenue)
-- ============================================================================
-- Use this if you want to see all mapping records, including those without matches
/*
CREATE OR REPLACE VIEW dataeng_stage.public.view_partner_finance_mapped AS
SELECT 
    m.ID,
    m.Oracle_Customer_Name,
    m.Oracle_Customer_Name_ID,
    m.Oracle_Invoice_Group,
    m.Oracle_Invoice_Name,
    m.Oracle_GL_Account,
    r.*
FROM dataeng_stage.public.mapping_template_raw_CURSOR m
LEFT JOIN BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
    ON m.ID = r.ID
    AND r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()));
*/

-- ============================================================================
-- Verify View Creation
-- ============================================================================
-- Check view definition
DESCRIBE VIEW dataeng_stage.public.view_partner_finance_mapped;

-- Check row count
SELECT COUNT(*) AS total_mapped_records
FROM dataeng_stage.public.view_partner_finance_mapped;

-- Preview sample data
SELECT *
FROM dataeng_stage.public.view_partner_finance_mapped
LIMIT 10;

-- ============================================================================
-- Check Prior Month Calculation
-- ============================================================================
-- Verify the prior month filter is working correctly
SELECT 
    CURRENT_DATE() AS current_date,
    DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE())) AS prior_month_start,
    DATE_TRUNC('MONTH', CURRENT_DATE()) AS current_month_start;

-- Check available months in revenue view
SELECT DISTINCT 
    data_month,
    COUNT(*) AS record_count
FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION
GROUP BY data_month
ORDER BY data_month DESC
LIMIT 12;

