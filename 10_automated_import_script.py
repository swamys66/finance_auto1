"""
Automated Finance Revenue Import Script

This Python script automates the execution of the finance revenue mapping process.
Can be run manually, via cron, or integrated into other automation systems.

Usage:
    python 10_automated_import_script.py [--dry-run] [--step STEP_NAME]

Steps:
    - import: Import CSV from S3
    - quality_raw: Run raw data quality checks
    - create_view: Create merged view
    - quality_merged: Run merged data quality checks
    - export: Export to S3
    - all: Run all steps (default)
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
import snowflake.connector
from snowflake.connector import DictCursor

# Configuration
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': 'dataeng_stage',
    'schema': 'public'
}

# SQL file paths
SCRIPT_DIR = Path(__file__).parent
SQL_FILES = {
    'import': SCRIPT_DIR / '02_import_from_s3.sql',
    'quality_raw': SCRIPT_DIR / '03_data_quality_checks_raw.sql',
    'create_view': SCRIPT_DIR / '04_create_mapped_view.sql',
    'quality_merged': SCRIPT_DIR / '05_data_quality_checks_merged.sql',
    'export': SCRIPT_DIR / '06_export_to_s3.sql',
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'finance_revenue_automation_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def read_sql_file(file_path: Path) -> str:
    """Read SQL file content."""
    try:
        with open(file_path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"SQL file not found: {file_path}")
        raise


def execute_sql_statements(conn, sql_content: str, step_name: str, dry_run: bool = False):
    """Execute SQL statements from file content."""
    if dry_run:
        logger.info(f"[DRY RUN] Would execute SQL for step: {step_name}")
        logger.debug(f"SQL content:\n{sql_content[:500]}...")  # Log first 500 chars
        return True
    
    cursor = conn.cursor()
    try:
        # Split by semicolon and execute each statement
        statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
        
        for i, statement in enumerate(statements, 1):
            if not statement or statement.startswith('/*'):
                continue
            
            # Skip commented sections
            if '/*' in statement and '*/' in statement:
                continue
            
            logger.info(f"Executing statement {i}/{len(statements)} for {step_name}")
            try:
                cursor.execute(statement)
                logger.info(f"Statement {i} executed successfully")
            except Exception as e:
                logger.error(f"Error executing statement {i}: {str(e)}")
                logger.error(f"Statement: {statement[:200]}...")
                raise
        
        conn.commit()
        logger.info(f"Step '{step_name}' completed successfully")
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error in step '{step_name}': {str(e)}")
        raise
    finally:
        cursor.close()


def validate_import(conn) -> bool:
    """Validate that import was successful."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM dataeng_stage.public.mapping_template_raw_CURSOR")
        row_count = cursor.fetchone()[0]
        
        if row_count == 0:
            logger.error("Validation failed: No rows loaded")
            return False
        
        logger.info(f"Validation passed: {row_count} rows loaded")
        return True
    finally:
        cursor.close()


def validate_view(conn) -> bool:
    """Validate that view was created successfully."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM dataeng_stage.public.view_partner_finance_mapped")
        row_count = cursor.fetchone()[0]
        
        if row_count == 0:
            logger.warning("View created but contains no rows (may be expected if no revenue data)")
            return True  # Not necessarily a failure
        
        logger.info(f"View validation passed: {row_count} rows in view")
        return True
    finally:
        cursor.close()


def run_step(conn, step_name: str, dry_run: bool = False) -> bool:
    """Run a specific step of the process."""
    logger.info(f"Starting step: {step_name}")
    
    if step_name not in SQL_FILES:
        logger.error(f"Unknown step: {step_name}")
        return False
    
    sql_file = SQL_FILES[step_name]
    sql_content = read_sql_file(sql_file)
    
    try:
        execute_sql_statements(conn, sql_content, step_name, dry_run)
        
        # Run validations after specific steps
        if step_name == 'import' and not dry_run:
            if not validate_import(conn):
                return False
        
        if step_name == 'create_view' and not dry_run:
            if not validate_view(conn):
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"Step '{step_name}' failed: {str(e)}")
        return False


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Automated Finance Revenue Import Script')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without executing SQL')
    parser.add_argument('--step', choices=['import', 'quality_raw', 'create_view', 'quality_merged', 'export', 'all'],
                       default='all', help='Step to execute (default: all)')
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Finance Revenue Automation Script")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'EXECUTION'}")
    logger.info(f"Step: {args.step}")
    logger.info("=" * 60)
    
    # Validate configuration
    if not all([SNOWFLAKE_CONFIG.get('user'), SNOWFLAKE_CONFIG.get('password'), SNOWFLAKE_CONFIG.get('account')]):
        logger.error("Snowflake configuration incomplete. Set SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT")
        sys.exit(1)
    
    # Connect to Snowflake
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        logger.info("Connected to Snowflake successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        sys.exit(1)
    
    try:
        # Set context
        cursor = conn.cursor()
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")
        cursor.close()
        
        # Define step order
        step_order = ['import', 'quality_raw', 'create_view', 'quality_merged', 'export']
        
        if args.step == 'all':
            # Run all steps in order
            success = True
            for step in step_order:
                if not run_step(conn, step, args.dry_run):
                    logger.error(f"Process failed at step: {step}")
                    success = False
                    break
                logger.info(f"Step '{step}' completed")
            
            if success:
                logger.info("=" * 60)
                logger.info("All steps completed successfully!")
                logger.info("=" * 60)
            else:
                logger.error("Process failed. Check logs for details.")
                sys.exit(1)
        else:
            # Run single step
            if run_step(conn, args.step, args.dry_run):
                logger.info(f"Step '{args.step}' completed successfully")
            else:
                logger.error(f"Step '{args.step}' failed")
                sys.exit(1)
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
    
    finally:
        conn.close()
        logger.info("Disconnected from Snowflake")


if __name__ == "__main__":
    main()

