# Exploratory Data Analysis - WEO PostgreSQL Database
# Run this code line by line or in sections for interactive exploration

import pandas as pd
import psycopg2
import numpy as np
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

# Load PostgreSQL credentials from .env file
load_dotenv()

# ============================================================================
# 1. DATABASE CONNECTION SETUP
# ============================================================================

# Function to get database connection
def get_connection():
    """Get PostgreSQL database connection using .env credentials."""
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return psycopg2.connect(database_url)
    
    return psycopg2.connect(
        host=os.getenv('PG_HOST', 'localhost'),
        port=os.getenv('PG_PORT', '5432'),
        database=os.getenv('PG_DATABASE', 'weo'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD')
    )

# Connect to the database
conn = get_connection()
cursor = conn.cursor()

print("Connected to PostgreSQL database!")

# ============================================================================
# 2. BASIC DATABASE OVERVIEW
# ============================================================================

print("\n=== DATABASE OVERVIEW ===")

# Check PostgreSQL version
cursor.execute("SELECT version();")
db_version = cursor.fetchone()[0]
print(f"PostgreSQL version: {db_version.split(',')[0]}")

# Get table sizes
cursor.execute("SELECT COUNT(*) FROM countries")
countries_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM indicators")
indicators_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM metrics")
metrics_count = cursor.fetchone()[0]

print(f"\nTable sizes:")
print(f"Countries: {countries_count}")
print(f"Indicators: {indicators_count:,}")
print(f"Metrics: {metrics_count}")

# Check database size
cursor.execute("""
    SELECT pg_size_pretty(pg_database_size(current_database())) as database_size
""")
db_size = cursor.fetchone()[0]
print(f"Database size: {db_size}")

# ============================================================================
# 3. EXPLORE COUNTRIES TABLE
# ============================================================================

print("\n=== COUNTRIES TABLE ===")

# Load countries as DataFrame
countries = pd.read_sql("SELECT * FROM countries ORDER BY name", conn)
print(f"Countries shape: {countries.shape}")
print(f"Columns: {list(countries.columns)}")

# Regional distribution
print("\nCountries by region:")
region_counts = pd.read_sql("""
    SELECT region7, COUNT(*) as count 
    FROM countries 
    WHERE region7 IS NOT NULL
    GROUP BY region7 
    ORDER BY count DESC
""", conn)
print(region_counts)

# Economic groups
print("\nCountries by economic group:")
econ_counts = pd.read_sql("""
    SELECT econ_group, COUNT(*) as count 
    FROM countries 
    WHERE econ_group IS NOT NULL
    GROUP BY econ_group 
    ORDER BY count DESC
""", conn)
print(econ_counts)

# Special groups
cursor.execute("SELECT COUNT(*) FROM countries WHERE group_g7 = true")
g7_count = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM countries WHERE group_european_union = true")
eu_count = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM countries WHERE group_asean5 = true")
asean_count = cursor.fetchone()[0]

print(f"\nSpecial groups:")
print(f"G7 countries: {g7_count}")
print(f"EU countries: {eu_count}")
print(f"ASEAN5 countries: {asean_count}")

# ============================================================================
# 4. EXPLORE METRICS TABLE
# ============================================================================

print("\n=== METRICS TABLE ===")

# Load metrics as DataFrame
metrics = pd.read_sql("SELECT * FROM metrics ORDER BY subject_code", conn)
print(f"Metrics shape: {metrics.shape}")

# Most common units
print("\nMost common units:")
units_counts = pd.read_sql("""
    SELECT units, COUNT(*) as count 
    FROM metrics 
    WHERE units IS NOT NULL
    GROUP BY units 
    ORDER BY count DESC 
    LIMIT 10
""", conn)
print(units_counts)

# Most common scales
print("\nMost common scales:")
scale_counts = pd.read_sql("""
    SELECT scale, COUNT(*) as count 
    FROM metrics 
    WHERE scale IS NOT NULL
    GROUP BY scale 
    ORDER BY count DESC
""", conn)
print(scale_counts)

# Sample metrics
print("\nSample metrics:")
sample_metrics = pd.read_sql("""
    SELECT subject_code, description 
    FROM metrics 
    WHERE description IS NOT NULL
    ORDER BY subject_code 
    LIMIT 10
""", conn)
print(sample_metrics)

# ============================================================================
# 5. EXPLORE INDICATORS TABLE
# ============================================================================

print("\n=== INDICATORS TABLE ===")

# Year range and coverage
year_stats = pd.read_sql("""
    SELECT 
        MIN(year) as min_year, 
        MAX(year) as max_year,
        COUNT(DISTINCT year) as num_years,
        COUNT(DISTINCT iso_code) as num_countries,
        COUNT(DISTINCT subject_code) as num_metrics
    FROM indicators
""", conn)
print("Data coverage:")
print(f"Years: {year_stats.iloc[0]['min_year']} - {year_stats.iloc[0]['max_year']} ({year_stats.iloc[0]['num_years']} years)")
print(f"Countries: {year_stats.iloc[0]['num_countries']}")
print(f"Metrics: {year_stats.iloc[0]['num_metrics']}")

# Most common metrics
print("\nTop 10 most common subject codes:")
top_subjects = pd.read_sql("""
    SELECT subject_code, COUNT(*) as count 
    FROM indicators 
    GROUP BY subject_code 
    ORDER BY count DESC 
    LIMIT 10
""", conn)
print(top_subjects)

# Countries with most data
print("\nTop 10 countries with most data points:")
top_countries = pd.read_sql("""
    SELECT c.name, COUNT(*) as data_points
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    GROUP BY c.name
    ORDER BY data_points DESC
    LIMIT 10
""", conn)
print(top_countries)

# ============================================================================
# 6. SAMPLE ANALYSIS - GDP DATA
# ============================================================================

print("\n=== GDP ANALYSIS ===")

# Get GDP data for major economies in recent years
gdp_data = pd.read_sql("""
    SELECT c.name, i.year, i.value, m.units
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code  
    JOIN metrics m ON i.subject_code = m.subject_code
    WHERE i.subject_code = 'NGDPD'  -- GDP in USD billions
      AND i.year >= 2020
      AND c.iso_code IN ('USA', 'CHN', 'JPN', 'DEU', 'GBR', 'IND', 'FRA', 'ITA', 'BRA', 'CAN')
    ORDER BY i.year, i.value DESC
""", conn)

print("GDP data for major economies (2020+):")
print(gdp_data)

# GDP rankings for latest year
print("\nTop 10 economies by GDP (latest year):")
gdp_rankings = pd.read_sql("""
    SELECT c.name, i.year, i.value as gdp_usd_billions
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    WHERE i.subject_code = 'NGDPD'
      AND i.year = (SELECT MAX(year) FROM indicators WHERE subject_code = 'NGDPD')
    ORDER BY i.value DESC
    LIMIT 10
""", conn)
print(gdp_rankings)

# ============================================================================
# 7. TIME SERIES ANALYSIS
# ============================================================================

print("\n=== TIME SERIES ANALYSIS ===")

# GDP growth rates for G7 countries
growth_data = pd.read_sql("""
    SELECT c.name, i.year, i.value as gdp_growth_rate
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    WHERE i.subject_code = 'NGDP_RPCH'  -- GDP growth rate
      AND c.group_g7 = true
      AND i.year >= 2010
    ORDER BY c.name, i.year
""", conn)

print(f"GDP growth data shape: {growth_data.shape}")
print("Sample GDP growth rates:")
print(growth_data.head(10))

# Average GDP growth by G7 country (2010+)
avg_growth = pd.read_sql("""
    SELECT c.name, 
           AVG(i.value) as avg_growth_rate,
           MIN(i.value) as min_growth_rate,
           MAX(i.value) as max_growth_rate,
           COUNT(*) as years_of_data
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    WHERE i.subject_code = 'NGDP_RPCH'
      AND c.group_g7 = true
      AND i.year >= 2010
    GROUP BY c.name
    ORDER BY avg_growth_rate DESC
""", conn)

print("\nG7 Average GDP Growth Rates (2010+):")
print(avg_growth.round(2))

# ============================================================================
# 8. REGIONAL COMPARISON
# ============================================================================

print("\n=== REGIONAL COMPARISON ===")

# Average GDP per capita by region (latest year)
gdp_per_capita = pd.read_sql("""
    SELECT c.region7, 
           AVG(i.value) as avg_gdp_per_capita, 
           COUNT(*) as country_count,
           MIN(i.value) as min_gdp_per_capita,
           MAX(i.value) as max_gdp_per_capita
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    WHERE i.subject_code = 'NGDPDPC'  -- GDP per capita in USD
      AND i.year = (SELECT MAX(year) FROM indicators WHERE subject_code = 'NGDPDPC')
      AND c.region7 IS NOT NULL
    GROUP BY c.region7
    ORDER BY avg_gdp_per_capita DESC
""", conn)

print("GDP per capita by region (latest year):")
print(gdp_per_capita.round(0))

# ============================================================================
# 9. DATA QUALITY CHECKS
# ============================================================================

print("\n=== DATA QUALITY ===")

# Check for missing data patterns
missing_by_year = pd.read_sql("""
    SELECT year, 
           COUNT(*) as observations,
           COUNT(DISTINCT iso_code) as countries,
           COUNT(DISTINCT subject_code) as metrics
    FROM indicators
    GROUP BY year
    ORDER BY year
""", conn)

print("Data availability by year (recent years):")
print(missing_by_year.tail(10))

# Check value distributions
value_stats = pd.read_sql("""
    SELECT 
        MIN(value) as min_val, 
        MAX(value) as max_val, 
        AVG(value) as avg_val,
        STDDEV(value) as std_val,
        COUNT(*) as total_obs,
        COUNT(DISTINCT iso_code) as unique_countries,
        COUNT(DISTINCT subject_code) as unique_metrics
    FROM indicators
""", conn)

print("\nValue statistics:")
print(f"Min value: {value_stats.iloc[0]['min_val']}")
print(f"Max value: {value_stats.iloc[0]['max_val']:,.2f}")
print(f"Avg value: {value_stats.iloc[0]['avg_val']:.2f}")
print(f"Std deviation: {value_stats.iloc[0]['std_val']:.2f}")
print(f"Total observations: {value_stats.iloc[0]['total_obs']:,}")

# ============================================================================
# 10. INTERESTING PATTERNS
# ============================================================================

print("\n=== INTERESTING PATTERNS ===")

# Countries with highest inflation in recent years
high_inflation = pd.read_sql("""
    SELECT c.name, i.year, i.value as inflation_rate, c.region7
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    WHERE i.subject_code = 'PCPIPCH'  -- Inflation rate
      AND i.year >= 2020
      AND i.value > 20  -- High inflation threshold
    ORDER BY i.value DESC
    LIMIT 15
""", conn)

print("Countries with highest inflation (>20%, 2020+):")
print(high_inflation)

# Unemployment rates by region (latest year)
unemployment_by_region = pd.read_sql("""
    SELECT c.region7,
           AVG(i.value) as avg_unemployment_rate,
           MIN(i.value) as min_unemployment_rate,
           MAX(i.value) as max_unemployment_rate,
           COUNT(*) as countries_with_data
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    WHERE i.subject_code = 'LUR'  -- Unemployment rate
      AND i.year = (SELECT MAX(year) FROM indicators WHERE subject_code = 'LUR')
      AND c.region7 IS NOT NULL
    GROUP BY c.region7
    ORDER BY avg_unemployment_rate DESC
""", conn)

print("\nUnemployment rates by region (latest year):")
print(unemployment_by_region.round(2))

# ============================================================================
# 11. ADVANCED QUERIES - ECONOMIC CORRELATIONS
# ============================================================================

print("\n=== ECONOMIC CORRELATIONS ===")

# Countries with both high GDP per capita and low unemployment (latest year)
economic_performance = pd.read_sql("""
    SELECT c.name, c.region7,
           gdp.value as gdp_per_capita,
           unemp.value as unemployment_rate
    FROM countries c
    LEFT JOIN indicators gdp ON c.iso_code = gdp.iso_code 
        AND gdp.subject_code = 'NGDPDPC'
        AND gdp.year = (SELECT MAX(year) FROM indicators WHERE subject_code = 'NGDPDPC')
    LEFT JOIN indicators unemp ON c.iso_code = unemp.iso_code 
        AND unemp.subject_code = 'LUR'
        AND unemp.year = (SELECT MAX(year) FROM indicators WHERE subject_code = 'LUR')
    WHERE gdp.value IS NOT NULL AND unemp.value IS NOT NULL
      AND gdp.value > 30000  -- High GDP per capita threshold
      AND unemp.value < 5    -- Low unemployment threshold
    ORDER BY gdp.value DESC
""", conn)

print("High-performing economies (GDP per capita > $30k, unemployment < 5%):")
print(economic_performance)

# ============================================================================
# 12. SETUP FOR FURTHER ANALYSIS
# ============================================================================

print("\n=== READY FOR FURTHER ANALYSIS ===")
print("Variables available for continued exploration:")
print("- conn: PostgreSQL connection")
print("- cursor: Database cursor")
print("- countries: Countries DataFrame") 
print("- metrics: Metrics DataFrame")
print("- gdp_data: Major economies GDP data")
print("- growth_data: G7 GDP growth rates")
print("- gdp_per_capita: Regional GDP per capita")
print("- missing_by_year: Data availability by year")

print("\nUseful SQL patterns for further exploration:")
print("# Get specific country data:")
print("# pd.read_sql(\"SELECT * FROM indicators WHERE iso_code = 'USA'\", conn)")
print("\n# Join all tables:")
print("# pd.read_sql(\"\"\"")
print("#     SELECT c.name, m.description, i.year, i.value")
print("#     FROM indicators i")
print("#     JOIN countries c ON i.iso_code = c.iso_code")
print("#     JOIN metrics m ON i.subject_code = m.subject_code")
print("#     WHERE i.year = 2023")
print("# \"\"\", conn)")

print("\nDatabase connection remains open for further queries...")

# Uncomment to close connection when done
# cursor.close()
# conn.close()