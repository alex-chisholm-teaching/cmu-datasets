#!/usr/bin/env python3
"""
WEO Database Creation Script

Creates a DuckDB database from World Economic Outlook CSV files:
- countries.csv -> countries table
- indicators.csv -> indicators table  
- metrics.csv -> metrics table
"""

import duckdb
import pandas as pd
from pathlib import Path

def create_weo_database():
    """Create WEO database from CSV files."""
    
    # Connect to DuckDB (creates file if doesn't exist)
    conn = duckdb.connect('weo.db')
    
    print("Creating WEO database...")
    
    # Load countries table
    print("Loading countries table...")
    countries_df = pd.read_csv('countries.csv')
    conn.execute("DROP TABLE IF EXISTS countries")
    conn.execute("""
        CREATE TABLE countries (
            country_id INTEGER PRIMARY KEY,
            iso_code VARCHAR(3),
            name VARCHAR(100),
            region7 VARCHAR(50),
            econ_group VARCHAR(50),
            group_g7 BOOLEAN,
            group_european_union BOOLEAN,
            group_asean5 BOOLEAN
        )
    """)
    conn.execute("INSERT INTO countries SELECT * FROM countries_df")
    print(f"  Inserted {len(countries_df)} countries")
    
    # Load indicators table (actual data)
    print("Loading indicators table...")
    indicators_df = pd.read_csv('metrics.csv')  # This has the actual data
    
    # Clean the data - replace '--' and other non-numeric values with NaN
    indicators_df['value'] = pd.to_numeric(indicators_df['value'], errors='coerce')
    
    # Remove rows with missing values
    indicators_df = indicators_df.dropna(subset=['value'])
    
    conn.execute("DROP TABLE IF EXISTS indicators")
    conn.execute("""
        CREATE TABLE indicators (
            metric_id INTEGER PRIMARY KEY,
            iso_code VARCHAR(3),
            subject_code VARCHAR(20),
            year INTEGER,
            value DOUBLE
        )
    """)
    conn.execute("INSERT INTO indicators SELECT * FROM indicators_df")
    print(f"  Inserted {len(indicators_df)} observations")
    
    # Load metrics table (definitions)
    print("Loading metrics table...")
    metrics_df = pd.read_csv('indicators.csv')  # This has the definitions
    conn.execute("DROP TABLE IF EXISTS metrics")
    conn.execute("""
        CREATE TABLE metrics (
            indicator_id INTEGER PRIMARY KEY,
            subject_code VARCHAR(20),
            description TEXT,
            notes TEXT,
            units VARCHAR(100),
            scale VARCHAR(50)
        )
    """)
    conn.execute("INSERT INTO metrics SELECT * FROM metrics_df")
    print(f"  Inserted {len(metrics_df)} metric definitions")
    
    # Create indexes for better query performance
    print("Creating indexes...")
    conn.execute("CREATE INDEX idx_countries_iso ON countries(iso_code)")
    conn.execute("CREATE INDEX idx_indicators_iso ON indicators(iso_code)")
    conn.execute("CREATE INDEX idx_indicators_subject ON indicators(subject_code)")
    conn.execute("CREATE INDEX idx_indicators_year ON indicators(year)")
    conn.execute("CREATE INDEX idx_metrics_subject ON metrics(subject_code)")
    
    # Display database info
    print("\nDatabase created successfully!")
    print(f"Database file: {Path('weo.db').absolute()}")
    
    # Show table counts
    countries_count = conn.execute("SELECT COUNT(*) FROM countries").fetchone()[0]
    indicators_count = conn.execute("SELECT COUNT(*) FROM indicators").fetchone()[0]
    metrics_count = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
    
    print(f"\nTable summary:")
    print(f"  countries: {countries_count} rows")
    print(f"  indicators: {indicators_count} rows")
    print(f"  metrics: {metrics_count} rows")
    
    # Example query
    print(f"\nSample query - GDP data for USA in 2023:")
    result = conn.execute("""
        SELECT c.name, m.description, i.value, m.units
        FROM indicators i
        JOIN countries c ON i.iso_code = c.iso_code
        JOIN metrics m ON i.subject_code = m.subject_code
        WHERE c.iso_code = 'USA' 
          AND i.year = 2023 
          AND i.subject_code = 'NGDPD'
    """).fetchall()
    
    for row in result:
        print(f"  {row[0]}: {row[1]} = {row[2]} {row[3]}")
    
    conn.close()

if __name__ == "__main__":
    create_weo_database()