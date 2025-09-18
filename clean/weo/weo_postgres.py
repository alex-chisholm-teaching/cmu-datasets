#!/usr/bin/env python3
"""
WEO PostgreSQL Database Creation Script

Creates PostgreSQL database tables from World Economic Outlook CSV files:
- countries.csv -> countries table
- indicators.csv -> indicators table (definitions)
- metrics.csv -> metrics table (actual data)

Requires .env file with PostgreSQL credentials:
DATABASE_URL=postgresql://user:password@host:port/database
or
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=weo
PG_USER=username  
PG_PASSWORD=password
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pathlib import Path

def load_postgres_credentials():
    """Load PostgreSQL credentials from .env file."""
    load_dotenv()
    
    # Try DATABASE_URL first (common in production)
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return database_url
    
    # Otherwise build from individual components
    config = {
        'host': os.getenv('PG_HOST', 'localhost'),
        'port': os.getenv('PG_PORT', '5432'),
        'database': os.getenv('PG_DATABASE', 'weo'),
        'user': os.getenv('PG_USER'),
        'password': os.getenv('PG_PASSWORD')
    }
    
    # Check required fields
    if not config['user'] or not config['password']:
        raise ValueError("Missing required PostgreSQL credentials in .env file")
    
    return config

def create_weo_postgres_database():
    """Create WEO database tables in PostgreSQL from CSV files."""
    
    print("Loading PostgreSQL credentials...")
    pg_config = load_postgres_credentials()
    
    # Connect to PostgreSQL
    if isinstance(pg_config, str):
        # DATABASE_URL format
        conn = psycopg2.connect(pg_config)
    else:
        # Individual parameters
        conn = psycopg2.connect(**pg_config)
    
    cursor = conn.cursor()
    
    print("Creating WEO database tables in PostgreSQL...")
    
    try:
        # =====================================================================
        # 1. CREATE COUNTRIES TABLE
        # =====================================================================
        print("Creating countries table...")
        
        cursor.execute("DROP TABLE IF EXISTS countries CASCADE")
        cursor.execute("""
            CREATE TABLE countries (
                country_id SERIAL PRIMARY KEY,
                iso_code VARCHAR(3) UNIQUE NOT NULL,
                name VARCHAR(100) NOT NULL,
                region7 VARCHAR(50),
                econ_group VARCHAR(50),
                group_g7 BOOLEAN DEFAULT FALSE,
                group_european_union BOOLEAN DEFAULT FALSE,
                group_asean5 BOOLEAN DEFAULT FALSE
            )
        """)
        
        # Load and clean countries data
        countries_df = pd.read_csv('countries.csv')
        
        # First, remove rows with missing essential data (iso_code or name)
        countries_df = countries_df.dropna(subset=['iso_code', 'name'])
        
        # Remove any rows where iso_code is empty string
        countries_df = countries_df[countries_df['iso_code'].str.strip() != '']
        
        # Clean the data - handle missing values
        # For text columns, replace NaN with empty string first, then convert to None for SQL
        text_cols = ['region7', 'econ_group']
        for col in text_cols:
            if col in countries_df.columns:
                countries_df[col] = countries_df[col].fillna('')
        
        # For boolean columns, fill NaN with False and convert to proper booleans
        bool_cols = ['group_g7', 'group_european_union', 'group_asean5']
        for col in bool_cols:
            if col in countries_df.columns:
                countries_df[col] = countries_df[col].fillna(False)
                # Convert to boolean, handling any remaining NaN or string values
                countries_df[col] = countries_df[col].astype(str).str.lower().isin(['true', '1', 'yes'])
        
        # Convert empty strings to None for SQL NULL
        countries_df = countries_df.replace('', None)
        
        countries_data = [tuple(row) for row in countries_df.values]
        
        execute_values(
            cursor,
            """INSERT INTO countries (country_id, iso_code, name, region7, econ_group, 
               group_g7, group_european_union, group_asean5) VALUES %s""",
            countries_data
        )
        print(f"  Inserted {len(countries_df)} countries")
        
        # =====================================================================
        # 2. CREATE INDICATORS TABLE (definitions)
        # =====================================================================
        print("Creating indicators table...")
        
        cursor.execute("DROP TABLE IF EXISTS indicators CASCADE")
        cursor.execute("""
            CREATE TABLE indicators (
                indicator_id SERIAL PRIMARY KEY,
                subject_code VARCHAR(20) UNIQUE NOT NULL,
                description TEXT,
                notes TEXT,
                units VARCHAR(100),
                scale VARCHAR(50)
            )
        """)
        
        # Load and clean metrics data
        indicators_df = pd.read_csv('indicators.csv')  # This has the definitions
        
        # Remove rows with missing essential data (subject_code)
        indicators_df = indicators_df.dropna(subset=['subject_code'])
        
        # Remove any rows where subject_code is empty string
        if 'subject_code' in indicators_df.columns:
            indicators_df = indicators_df[indicators_df['subject_code'].astype(str).str.strip() != '']
        
        # Clean the data - handle missing values
        text_cols = ['description', 'notes', 'units', 'scale']
        for col in text_cols:
            if col in indicators_df.columns:
                indicators_df[col] = indicators_df[col].fillna('')
        
        # Convert empty strings to None for SQL NULL
        indicators_df = indicators_df.replace('', None)
        
        indicators_data = [tuple(row) for row in indicators_df.values]
        
        execute_values(
            cursor,
            """INSERT INTO indicators (indicator_id, subject_code, description, notes, units, scale) VALUES %s""",
            indicators_data
        )
        print(f"  Inserted {len(indicators_df)} metric definitions")
        
        # =====================================================================
        # 3. CREATE METRICS TABLE (actual data)
        # =====================================================================
        print("Creating metrics table...")
        
        cursor.execute("DROP TABLE IF EXISTS metrics CASCADE")
        cursor.execute("""
            CREATE TABLE metrics (
                metric_id SERIAL PRIMARY KEY,
                iso_code VARCHAR(3) NOT NULL REFERENCES countries(iso_code),
                subject_code VARCHAR(20) NOT NULL REFERENCES indicators(subject_code),
                year INTEGER NOT NULL,
                value NUMERIC,
                UNIQUE(iso_code, subject_code, year)
            )
        """)
        
        # Load and clean indicators data
        metrics_df = pd.read_csv('metrics.csv')  # This has the actual data
        
        # Clean the data - replace '--' and other non-numeric values with NaN
        metrics_df['value'] = pd.to_numeric(metrics_df['value'], errors='coerce')
        
        # Remove rows with missing values
        metrics_df = metrics_df.dropna(subset=['value'])
        
        # Prepare data for insertion
        metrics_data = [tuple(row) for row in metrics_df.values]
        
        execute_values(
            cursor,
            """INSERT INTO metrics (metric_id, iso_code, subject_code, year, value) VALUES %s""",
            metrics_data
        )
        print(f"  Inserted {len(metrics_df)} observations")
        
        # =====================================================================
        # 4. CREATE INDEXES
        # =====================================================================
        print("Creating indexes...")
        
        cursor.execute("CREATE INDEX idx_countries_iso ON countries(iso_code)")
        cursor.execute("CREATE INDEX idx_indicators_subject ON indicators(subject_code)")
        cursor.execute("CREATE INDEX idx_metrics_iso ON metrics(iso_code)")
        cursor.execute("CREATE INDEX idx_metrics_subject ON metrics(subject_code)")
        cursor.execute("CREATE INDEX idx_metrics_year ON metrics(year)")
        cursor.execute("CREATE INDEX idx_metrics_value ON metrics(value)")
        
        # =====================================================================
        # 5. ADD FOREIGN KEY CONSTRAINTS (if not already added)
        # =====================================================================
        print("Adding additional constraints...")
        
        # Add check constraints
        cursor.execute("""
            ALTER TABLE metrics 
            ADD CONSTRAINT chk_year_range 
            CHECK (year >= 1980 AND year <= 2030)
        """)
        
        # Commit all changes
        conn.commit()
        
        # =====================================================================
        # 6. DISPLAY RESULTS
        # =====================================================================
        print("\nDatabase created successfully!")
        
        # Show table counts
        cursor.execute("SELECT COUNT(*) FROM countries")
        countries_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM indicators")
        indicators_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM metrics")
        metrics_count = cursor.fetchone()[0]
        
        print(f"\nTable summary:")
        print(f"  countries: {countries_count} rows")
        print(f"  indicators: {indicators_count:,} rows")
        print(f"  metrics: {metrics_count} rows")
        
        # Example query
        print(f"\nSample query - GDP data for USA in 2023:")
        cursor.execute("""
            SELECT c.name, i.description, m.value, i.units
            FROM metrics m
            JOIN countries c ON m.iso_code = c.iso_code
            JOIN indicators i ON m.subject_code = i.subject_code
            WHERE c.iso_code = 'USA' 
              AND m.year = 2023 
              AND m.subject_code = 'NGDPD'
        """)
        
        results = cursor.fetchall()
        for row in results:
            print(f"  {row[0]}: {row[1]} = {row[2]} {row[3]}")
        
        if not results:
            print("  No data found - try different year or country")
        
        # Show database connection info
        cursor.execute("SELECT version()")
        db_version = cursor.fetchone()[0]
        print(f"\nPostgreSQL version: {db_version}")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Check if required packages are installed
    try:
        import psycopg2
        import dotenv
    except ImportError as e:
        print(f"Missing required package: {e}")
        print("Install with: pip install psycopg2-binary python-dotenv")
        exit(1)
    
    create_weo_postgres_database()