#!/usr/bin/env python3
"""
Initialize Neon database with schema and date dimension
"""
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def execute_sql_file(conn, filepath, skip_if_exists=False):
    """Execute SQL commands from a file"""
    print(f"Executing {filepath}...")
    with open(filepath, 'r') as f:
        sql = f.read()
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print(f"✓ {filepath} completed")
    except psycopg2.errors.DuplicateTable:
        if skip_if_exists:
            print(f"⚠ Tables already exist, skipping {filepath}")
            conn.rollback()
        else:
            raise

def main():
    # Connect to Neon database
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL not found in .env file")
    
    print("Connecting to Neon database...")
    conn = psycopg2.connect(database_url)
    
    try:
        # Execute SQL files in order
        print("\nCreating dimension tables...")
        execute_sql_file(conn, 'sql/01_create_dimensions.sql', skip_if_exists=True)
        
        print("\nCreating fact tables...")
        execute_sql_file(conn, 'sql/02_create_facts.sql', skip_if_exists=True)
        
        print("\nPopulating date dimension...")
        execute_sql_file(conn, 'sql/03_populate_date_dimension.sql')
        
        print("\n✓ Database schema created successfully!")
        print("You can now run: python etl/load_sample_data.py")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    main()
