"""
Airflow DAG for Finance Revenue Mapping Process Automation

This DAG automates the complete finance revenue mapping process:
1. Import CSV mapping file from S3
2. Run data quality checks on raw data
3. Create merged view with revenue aggregation
4. Run data quality checks on merged view
5. Export final view to S3

Schedule: Daily at 2 AM (configurable)
"""

import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from pathlib import Path

# Configuration
ENV = os.getenv("RUN_ENV", "prod")
SNOWFLAKE_CONN_ID = f"snowflake/bi_{ENV}"
DAG_OWNER = "BI"
RETRIES = 2
RETRY_DELAY = 300  # 5 minutes

# SQL file paths (adjust based on your file structure)
SQL_BASE_PATH = Path(__file__).parent
SQL_FILES = {
    "import_s3": SQL_BASE_PATH / "02_import_from_s3.sql",
    "quality_raw": SQL_BASE_PATH / "03_data_quality_checks_raw.sql",
    "create_view": SQL_BASE_PATH / "04_create_mapped_view.sql",
    "quality_merged": SQL_BASE_PATH / "05_data_quality_checks_merged.sql",
    "export_s3": SQL_BASE_PATH / "06_export_to_s3.sql",
}


def read_sql_file(file_path: Path) -> str:
    """Read SQL file content."""
    try:
        with open(file_path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found: {file_path}")


def validate_import_success(**context):
    """Validate that the import was successful by checking row count."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = """
    SELECT COUNT(*) AS row_count
    FROM dataeng_stage.public.mapping_template_raw_CURSOR;
    """
    result = hook.get_first(sql)
    row_count = result[0] if result else 0
    
    if row_count == 0:
        raise ValueError("Import failed: No rows loaded into mapping table")
    
    print(f"Import successful: {row_count} rows loaded")
    return row_count


def validate_view_creation(**context):
    """Validate that the view was created and has data."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = """
    SELECT COUNT(*) AS row_count
    FROM dataeng_stage.public.view_partner_finance_mapped;
    """
    result = hook.get_first(sql)
    row_count = result[0] if result else 0
    
    if row_count == 0:
        raise ValueError("View creation failed: No rows in merged view")
    
    print(f"View created successfully: {row_count} rows")
    return row_count


# Default arguments
default_args = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": RETRIES,
    "retry_delay": RETRY_DELAY,
    "snowflake_conn_id": SNOWFLAKE_CONN_ID,
}

# DAG definition
with DAG(
    dag_id="finance_revenue_mapping_automation",
    start_date=datetime(2025, 1, 1, tz=timezone.utc),
    schedule="0 2 * * *",  # Daily at 2 AM UTC (adjust as needed)
    catchup=False,
    default_args=default_args,
    description="Automated finance revenue mapping process from S3 import to S3 export",
    tags=["finance", "revenue", "mapping", "automation"],
    doc_md=__doc__,
) as dag:

    # Start task
    start = EmptyOperator(
        task_id="start",
        doc_md="Start of finance revenue mapping process"
    )

    # Task 1: Import CSV from S3
    import_from_s3 = SnowflakeOperator(
        task_id="import_csv_from_s3",
        sql=read_sql_file(SQL_FILES["import_s3"]),
        doc_md="""
        Import CSV mapping file from S3 bucket into Snowflake.
        Creates external stage, table, and loads data.
        """
    )

    # Validation: Check import success
    validate_import = PythonOperator(
        task_id="validate_import",
        python_callable=validate_import_success,
        doc_md="Validate that CSV import was successful"
    )

    # Task 2: Run data quality checks on raw data
    quality_checks_raw = SnowflakeOperator(
        task_id="data_quality_checks_raw",
        sql=read_sql_file(SQL_FILES["quality_raw"]),
        doc_md="Run comprehensive data quality checks on raw mapping data"
    )

    # Task 3: Create merged view
    create_mapped_view = SnowflakeOperator(
        task_id="create_mapped_view",
        sql=read_sql_file(SQL_FILES["create_view"]),
        doc_md="Create merged view joining mapping table with revenue aggregation"
    )

    # Validation: Check view creation
    validate_view = PythonOperator(
        task_id="validate_view",
        python_callable=validate_view_creation,
        doc_md="Validate that merged view was created successfully"
    )

    # Task 4: Run data quality checks on merged view
    quality_checks_merged = SnowflakeOperator(
        task_id="data_quality_checks_merged",
        sql=read_sql_file(SQL_FILES["quality_merged"]),
        doc_md="Run data quality checks on merged view (mapping success analysis)"
    )

    # Task 5: Export to S3
    export_to_s3 = SnowflakeOperator(
        task_id="export_to_s3",
        sql=read_sql_file(SQL_FILES["export_s3"]),
        doc_md="Export final merged view to S3 bucket for data handoff"
    )

    # End task
    end = EmptyOperator(
        task_id="end",
        doc_md="End of finance revenue mapping process"
    )

    # Define task dependencies
    start >> import_from_s3 >> validate_import >> quality_checks_raw >> \
    create_mapped_view >> validate_view >> quality_checks_merged >> \
    export_to_s3 >> end

