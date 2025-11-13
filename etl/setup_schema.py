#!/usr/bin/env python3
"""
Database schema setup script for Airline Analytics Data Warehouse
"""
import psycopg2
import os
from pathlib import Path

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'airline_analytics'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres')
    )

def execute_sql_file(conn, filepath):
    """Execute SQL file"""
    print(f"Executing {filepath}...")
    with open(filepath, 'r') as f:
        sql = f.read()
    
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"✓ {filepath} completed")

def main():
    """Main setup function"""
    sql_dir = Path(__file__).parent.parent / 'sql'
    
    sql_files = [
        '01_create_dimensions.sql',
        '02_create_facts.sql',
        '03_populate_date_dimension.sql'
    ]
    
    try:
        conn = get_db_connection()
        print("Connected to database")
        
        for sql_file in sql_files:
            filepath = sql_dir / sql_file
            if filepath.exists():
                execute_sql_file(conn, filepath)
            else:
                print(f"Warning: {sql_file} not found")
        
        print("\n✓ Schema setup completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
