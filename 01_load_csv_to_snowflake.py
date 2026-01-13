"""
Script to load CSV file into Snowflake table: mapping_template_raw_CURSOR

Prerequisites:
- snowflake-connector-python installed
- Snowflake credentials configured
- CSV file with required columns in order:
  1. ID (VARCHAR)
  2. Oracle_Customer_Name (VARCHAR)
  3. Oracle_Customer_Name_ID (VARCHAR)
  4. Oracle_Invoice_Group (VARCHAR)
  5. Oracle_Invoice_Name (VARCHAR)
  6. Oracle_GL_Account (VARCHAR)
"""

import snowflake.connector
import pandas as pd
import os
from pathlib import Path

# Configuration
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': 'dev_data_ingress',
    'schema': 'finance'
}

TABLE_NAME = 'mapping_template_raw_CURSOR'
CSV_FILE_PATH = 'path/to/your/mapping_file.csv'  # Update with actual path


def create_table_if_not_exists(conn, cursor):
    """Create the mapping table if it doesn't exist."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{TABLE_NAME} (
        ID VARCHAR,
        Oracle_Customer_Name VARCHAR,
        Oracle_Customer_Name_ID VARCHAR,
        Oracle_Invoice_Group VARCHAR,
        Oracle_Invoice_Name VARCHAR,
        Oracle_GL_Account VARCHAR
    );
    """
    cursor.execute(create_table_sql)
    print(f"Table {TABLE_NAME} created/verified successfully")


def load_csv_to_snowflake(csv_path, conn, cursor):
    """Load CSV file into Snowflake table."""
    # Read CSV file
    df = pd.read_csv(csv_path)
    
    # Validate columns
    expected_columns = [
        'ID', 'Oracle_Customer_Name', 'Oracle_Customer_Name_ID',
        'Oracle_Invoice_Group', 'Oracle_Invoice_Name', 'Oracle_GL_Account'
    ]
    
    if list(df.columns) != expected_columns:
        raise ValueError(
            f"CSV columns don't match expected order. "
            f"Expected: {expected_columns}, Got: {list(df.columns)}"
        )
    
    print(f"CSV file loaded: {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    
    # Truncate existing table
    truncate_sql = f"TRUNCATE TABLE {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{TABLE_NAME};"
    cursor.execute(truncate_sql)
    print("Table truncated")
    
    # Convert DataFrame to list of tuples for insertion
    values = [tuple(row) for row in df.values]
    
    # Insert data
    insert_sql = f"""
    INSERT INTO {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{TABLE_NAME}
    (ID, Oracle_Customer_Name, Oracle_Customer_Name_ID, 
     Oracle_Invoice_Group, Oracle_Invoice_Name, Oracle_GL_Account)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    cursor.executemany(insert_sql, values)
    print(f"Inserted {len(values)} rows into {TABLE_NAME}")
    
    # Verify row count
    cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{TABLE_NAME}")
    row_count = cursor.fetchone()[0]
    print(f"Verification: {row_count} rows in table")


def main():
    """Main execution function."""
    # Validate CSV file exists
    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}")
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Set context
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")
        
        # Create table
        create_table_if_not_exists(conn, cursor)
        
        # Load CSV
        load_csv_to_snowflake(CSV_FILE_PATH, conn, cursor)
        
        # Commit transaction
        conn.commit()
        print("Data loaded successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"Error: {str(e)}")
        raise
    
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()

