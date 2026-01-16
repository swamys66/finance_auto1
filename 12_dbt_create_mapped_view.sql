{{
    config(
        materialized='view',
        schema='finance',
        database='dev_data_ingress',
        tags=['finance', 'revenue', 'mapping', 'mapped_view'],
        description='Creates a mapped view joining revenue aggregation with mapping template, filtered for 4 months prior data'
    )
}}

-- DBT Model: Create Mapped Finance Revenue View
-- This model joins the revenue aggregation view with the mapping template
-- Filters for data_month = 4 months prior to current date
-- 
-- Logic:
-- - LEFT JOIN ensures all revenue records are included
-- - Mapping fields may be NULL if no mapping exists
-- - Includes validation flags to identify mapped vs unmapped records

SELECT 
    -- Mapping fields from mapping_template_raw_CURSOR (may be NULL if no mapping)
    m.Oracle_Customer_Name,
    m.Oracle_Customer_Name_ID,
    m.Oracle_Invoice_Group,
    m.Oracle_Invoice_Name,
    m.Oracle_GL_Account,

    -- Flags for validation
    CASE 
        WHEN m.Oracle_GL_Account IS NOT NULL THEN 'Y' 
        ELSE 'N' 
    END AS Oracle_mapped_record_flag,
    CASE 
        WHEN m.ID IS NOT NULL THEN 'Y' 
        ELSE 'N' 
    END AS ID_mapped_record_flag,
    
    -- All fields from revenue aggregation view (master)
    r.*
    
FROM BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION r
LEFT JOIN {{ ref('_1_import_from_s3') }} m
    ON r.ID = m.ID
WHERE r.data_month = DATE_TRUNC('MONTH', DATEADD(MONTH, -4, CURRENT_DATE()))

