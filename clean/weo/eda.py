# Exploratory Data Analysis - WEO Database
# Run this code line by line or in sections for interactive exploration

import pandas as pd
import duckdb
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to the database
conn = duckdb.connect('weo.db')

# ============================================================================
# 1. BASIC DATABASE OVERVIEW
# ============================================================================

# Check table sizes
print("=== DATABASE OVERVIEW ===")
countries_count = conn.execute("SELECT COUNT(*) FROM countries").fetchone()[0]
indicators_count = conn.execute("SELECT COUNT(*) FROM indicators").fetchone()[0] 
metrics_count = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]

print(f"Countries: {countries_count}")
print(f"Indicators: {indicators_count:,}")
print(f"Metrics: {metrics_count}")

# ============================================================================
# 2. EXPLORE COUNTRIES TABLE
# ============================================================================

print("\n=== COUNTRIES TABLE ===")

# Load countries as DataFrame
countries = conn.execute("SELECT * FROM countries").df()
print(f"Countries shape: {countries.shape}")
print(f"Columns: {list(countries.columns)}")

# Regional distribution
print("\nCountries by region:")
print(countries['region7'].value_counts())

# Economic groups
print("\nCountries by economic group:")
print(countries['econ_group'].value_counts())

# Special groups
print(f"\nG7 countries: {countries['group_g7'].sum()}")
print(f"EU countries: {countries['group_european_union'].sum()}")
print(f"ASEAN5 countries: {countries['group_asean5'].sum()}")

# ============================================================================
# 3. EXPLORE METRICS TABLE  
# ============================================================================

print("\n=== METRICS TABLE ===")

# Load metrics as DataFrame
metrics = conn.execute("SELECT * FROM metrics").df()
print(f"Metrics shape: {metrics.shape}")

# Most common units
print("\nMost common units:")
print(metrics['units'].value_counts().head(10))

# Most common scales
print("\nMost common scales:")
print(metrics['scale'].value_counts())

# Sample metrics
print("\nSample metrics:")
print(metrics[['subject_code', 'description']].head(10))

# ============================================================================
# 4. EXPLORE INDICATORS TABLE
# ============================================================================

print("\n=== INDICATORS TABLE ===")

# Basic stats on indicators
print("Year range:")
year_stats = conn.execute("SELECT MIN(year) as min_year, MAX(year) as max_year, COUNT(DISTINCT year) as num_years FROM indicators").fetchone()
print(f"Years: {year_stats[0]} - {year_stats[1]} ({year_stats[2]} years)")

# Most common metrics
print("\nTop 10 most common subject codes:")
top_subjects = conn.execute("""
    SELECT subject_code, COUNT(*) as count 
    FROM indicators 
    GROUP BY subject_code 
    ORDER BY count DESC 
    LIMIT 10
""").df()
print(top_subjects)

# Countries with most data
print("\nTop 10 countries with most data points:")
top_countries = conn.execute("""
    SELECT c.name, COUNT(*) as data_points
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    GROUP BY c.name
    ORDER BY data_points DESC
    LIMIT 10
""").df()
print(top_countries)

# ============================================================================
# 5. SAMPLE ANALYSIS - GDP DATA
# ============================================================================

print("\n=== GDP ANALYSIS ===")

# Get GDP data for major economies in recent years
gdp_data = conn.execute("""
    SELECT c.name, i.year, i.value, m.units
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code  
    JOIN metrics m ON i.subject_code = m.subject_code
    WHERE i.subject_code = 'NGDPD'  -- GDP in USD billions
      AND i.year >= 2020
      AND c.iso_code IN ('USA', 'CHN', 'JPN', 'DEU', 'GBR', 'IND', 'FRA', 'ITA', 'BRA', 'CAN')
    ORDER BY i.year, i.value DESC
""").df()

print("GDP data for major economies (2020+):")
print(gdp_data)

# ============================================================================
# 6. TIME SERIES ANALYSIS
# ============================================================================

print("\n=== TIME SERIES ANALYSIS ===")

# GDP growth rates for G7 countries
growth_data = conn.execute("""
    SELECT c.name, i.year, i.value
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    WHERE i.subject_code = 'NGDP_RPCH'  -- GDP growth rate
      AND c.group_g7 = true
      AND i.year >= 2010
    ORDER BY c.name, i.year
""").df()

print(f"GDP growth data shape: {growth_data.shape}")
print("Sample GDP growth rates:")
print(growth_data.head(10))

# Pivot for easier analysis
if not growth_data.empty:
    growth_pivot = growth_data.pivot(index='year', columns='name', values='value')
    print("\nGDP Growth Rates (%) - G7 Countries:")
    print(growth_pivot.round(2))

# ============================================================================
# 7. REGIONAL COMPARISON
# ============================================================================

print("\n=== REGIONAL COMPARISON ===")

# Average GDP per capita by region (latest year)
gdp_per_capita = conn.execute("""
    SELECT c.region7, AVG(i.value) as avg_gdp_per_capita, COUNT(*) as country_count
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    WHERE i.subject_code = 'NGDPDPC'  -- GDP per capita in USD
      AND i.year = (SELECT MAX(year) FROM indicators WHERE subject_code = 'NGDPDPC')
    GROUP BY c.region7
    ORDER BY avg_gdp_per_capita DESC
""").df()

print("Average GDP per capita by region (latest year):")
print(gdp_per_capita.round(0))

# ============================================================================
# 8. DATA QUALITY CHECKS
# ============================================================================

print("\n=== DATA QUALITY ===")

# Check for missing data patterns
missing_by_year = conn.execute("""
    SELECT year, COUNT(*) as observations,
           COUNT(DISTINCT iso_code) as countries,
           COUNT(DISTINCT subject_code) as metrics
    FROM indicators
    GROUP BY year
    ORDER BY year
""").df()

print("Data availability by year:")
print(missing_by_year.tail(10))

# Check value distributions
print("\nValue statistics:")
value_stats = conn.execute("SELECT MIN(value) as min_val, MAX(value) as max_val, AVG(value) as avg_val, COUNT(*) as total_obs FROM indicators").fetchone()
print(f"Min value: {value_stats[0]}")
print(f"Max value: {value_stats[1]:,.2f}")
print(f"Avg value: {value_stats[2]:.2f}")
print(f"Total observations: {value_stats[3]:,}")

# ============================================================================
# 9. INTERESTING PATTERNS
# ============================================================================

print("\n=== INTERESTING PATTERNS ===")

# Countries with highest inflation in recent years
high_inflation = conn.execute("""
    SELECT c.name, i.year, i.value as inflation_rate
    FROM indicators i
    JOIN countries c ON i.iso_code = c.iso_code
    WHERE i.subject_code = 'PCPIPCH'  -- Inflation rate
      AND i.year >= 2020
      AND i.value > 20  -- High inflation threshold
    ORDER BY i.value DESC
    LIMIT 10
""").df()

print("Countries with highest inflation (>20%, 2020+):")
print(high_inflation)

# ============================================================================
# 10. SETUP FOR FURTHER ANALYSIS
# ============================================================================

print("\n=== READY FOR FURTHER ANALYSIS ===")
print("Variables available for continued exploration:")
print("- conn: DuckDB connection")
print("- countries: Countries DataFrame") 
print("- metrics: Metrics DataFrame")
print("- gdp_data: Major economies GDP data")
print("- growth_data: G7 GDP growth rates")
print("- gdp_per_capita: Regional GDP per capita")
print("- missing_by_year: Data availability by year")

print("\nDatabase connection remains open for further queries...")

# Close connection when done (uncomment to close)
# conn.close()