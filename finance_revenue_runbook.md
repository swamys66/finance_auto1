# Finance revenue run book

1. load an csv file in to snowflake schema for example: dev_data_ingress.finance
2. Following columns should be present in the order below from the imported CSV file in step 1
    a. ID VARCHAR,	
    b. Oracle_Customer_Name VARCHAR
    c. Oracle_Customer_Name_ID VARCHAR
    d. Oracle_Invoice_Group VARCHAR	
    e. Oracle_Invoice_Name VARCHAR
    f. Oracle_GL_Account VARCHAR
3. name the above table as mapping_template_raw_CURSOR
4. propose data quality checks to validate the above loaded raw mapping data
    a. for example: should have unique record by ID
5. Join the above mapping table to the following master snowflake view 
    a. BI.PARTNER_FINANCE.VIEW_PARTNER_FINANCE_REVENUE_AGGREGATION
    b. filter based on data_month = prior month
    c. for example, current month is January 2026, so the filter should be "data_month = '2025-12-01' "
    d. name the final merged view something like view_partned_finance_mapped
    e. Join key: ID 
6. propose data quality checks to validate the above merged table
    a. for example: what records mapped and what records did not get a mapping record
7. suggest an option to move this view to a S3 bucket for data hand off
