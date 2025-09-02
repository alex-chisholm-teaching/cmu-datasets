# Create DuckDB File from Three CSV Files - Step by Step Execution

import duckdb
import pandas as pd

# Step 1: Import DuckDB and connect to a database file
# This creates a new file called "titanic.duckdb" (or connects to existing one)
conn = duckdb.connect("titanic.duckdb")

# Step 2: Define your three Titanic CSV file paths
csv_file1 = "clean/titanic/titanic.csv"           # Main passenger data
csv_file2 = "clean/titanic/titanic_restaurant.csv"  # Restaurant/dining data
csv_file3 = "clean/titanic/titanic_luggage.csv"     # Luggage data

# Step 3: Create first table from main Titanic CSV file
# This reads the main passenger data and creates a table called 'passengers'
conn.execute("""
    CREATE TABLE passengers AS 
    SELECT * FROM read_csv('clean/titanic/titanic.csv')
""")

# Step 4: Verify the passengers table was created
result = conn.execute("SELECT COUNT(*) FROM passengers").fetchone()
print(f"Passengers table created with {result[0]} rows")

# Step 5: Look at the structure of the passengers table
schema = conn.execute("DESCRIBE passengers").fetchall()
print("Passengers table columns:")
for column in schema:
    print(f"  {column[0]} ({column[1]})")

# Step 6: Create second table from restaurant CSV file
conn.execute("""
    CREATE TABLE restaurant AS 
    SELECT * FROM read_csv('clean/titanic/titanic_restaurant.csv')
""")

# Step 7: Verify the restaurant table
result = conn.execute("SELECT COUNT(*) FROM restaurant").fetchone()
print(f"\nRestaurant table created with {result[0]} rows")

# Step 8: Create third table from luggage CSV file
conn.execute("""
    CREATE TABLE luggage AS 
    SELECT * FROM read_csv('clean/titanic/titanic_luggage.csv')
""")

# Step 9: Verify the luggage table
result = conn.execute("SELECT COUNT(*) FROM luggage").fetchone()
print(f"Luggage table created with {result[0]} rows")

# Step 10: Show all tables in the database
tables = conn.execute("SHOW TABLES").fetchall()
print(f"\nAll tables in database: {[table[0] for table in tables]}")

# Step 11: Test a simple query to make sure everything works
sample_data = conn.execute("SELECT * FROM passengers LIMIT 5").fetchall()
print(f"\nSample from passengers (first 5 rows): {len(sample_data)} rows")

# Step 12: Test queries on each table to see the passenger_id column
print("\nChecking passenger_id in each table...")

# Check passengers table
passenger_ids = conn.execute("SELECT passenger_id FROM passengers LIMIT 3").fetchall()
print(f"Sample passenger_ids from passengers table: {[row[0] for row in passenger_ids]}")

# Check restaurant table  
restaurant_ids = conn.execute("SELECT passenger_id FROM restaurant LIMIT 3").fetchall()
print(f"Sample passenger_ids from restaurant table: {[row[0] for row in restaurant_ids]}")

# Check luggage table
luggage_ids = conn.execute("SELECT passenger_id FROM luggage LIMIT 3").fetchall()
print(f"Sample passenger_ids from luggage table: {[row[0] for row in luggage_ids]}")

# Step 13: Test a join query across tables using passenger_id
print("\nTesting joins using passenger_id...")
joined_data = conn.execute("""
    SELECT 
        p.passenger_id,
        p.name,
        p.class,
        r.meal_preference,
        l.luggage_weight
    FROM passengers p
    LEFT JOIN restaurant r ON p.passenger_id = r.passenger_id
    LEFT JOIN luggage l ON p.passenger_id = l.passenger_id
    LIMIT 5
""").fetchall()

print(f"Joined data sample (5 rows):")
for row in joined_data:
    print(f"  {row}")

# Step 14: Close the connection (saves the database file)
conn.close()
print("\nTitanic database saved as 'titanic.duckdb'")

# Step 15: Test that the Titanic database file was created and can be reopened
print("\nTesting that the Titanic database file was saved correctly...")
test_conn = duckdb.connect("titanic.duckdb")
tables_check = test_conn.execute("SHOW TABLES").fetchall()
print(f"Reopened database contains tables: {[table[0] for table in tables_check]}")

# Check total passengers across all tables
total_passengers = test_conn.execute("SELECT COUNT(DISTINCT passenger_id) FROM passengers").fetchone()[0]
total_restaurant = test_conn.execute("SELECT COUNT(DISTINCT passenger_id) FROM restaurant").fetchone()[0] 
total_luggage = test_conn.execute("SELECT COUNT(DISTINCT passenger_id) FROM luggage").fetchone()[0]

print(f"Unique passengers in main table: {total_passengers}")
print(f"Unique passengers with restaurant data: {total_restaurant}")
print(f"Unique passengers with luggage data: {total_luggage}")

test_conn.close()

print("✅ Success! Your Titanic DuckDB database file is ready to use.")

# Alternative Step-by-Step Method: Using direct file paths in queries
print("\n" + "="*50)
print("ALTERNATIVE METHOD - Direct CSV reading for Titanic:")
print("="*50)

# Step A: Connect to a new database file
conn2 = duckdb.connect("titanic_direct.duckdb")

# Step B: Create tables directly with descriptive names
conn2.execute("CREATE TABLE titanic_passengers AS SELECT * FROM 'clean/titanic/titanic.csv'")
conn2.execute("CREATE TABLE titanic_dining AS SELECT * FROM 'clean/titanic/titanic_restaraunt.csv'")
conn2.execute("CREATE TABLE titanic_baggage AS SELECT * FROM 'clean/titanic/titanic_luggage.csv'")

# Step C: Verify all tables and test a comprehensive join
tables2 = conn2.execute("SHOW TABLES").fetchall()
print(f"Direct method created tables: {[table[0] for table in tables2]}")

# Test comprehensive Titanic analysis query
analysis_query = conn2.execute("""
    SELECT 
        p.passenger_id,
        p.name,
        p.survived,
        p.class,
        p.age,
        d.meal_preference,
        b.luggage_weight,
        CASE 
            WHEN d.passenger_id IS NOT NULL THEN 'Has dining data'
            ELSE 'No dining data'
        END as dining_status,
        CASE 
            WHEN b.passenger_id IS NOT NULL THEN 'Has luggage data'
            ELSE 'No luggage data' 
        END as luggage_status
    FROM titanic_passengers p
    LEFT JOIN titanic_dining d ON p.passenger_id = d.passenger_id
    LEFT JOIN titanic_baggage b ON p.passenger_id = b.passenger_id
    LIMIT 10
""").fetchall()

print("Comprehensive Titanic analysis (10 passengers):")
for row in analysis_query:
    print(f"  Passenger {row[0]}: {row[1]} - Survived: {row[2]} - Class: {row[3]}")

# Step D: Close the second database
conn2.close()

# Step-by-Step Method with Titanic-specific Error Handling
print("\n" + "="*50)
print("TITANIC METHOD WITH ERROR HANDLING:")
print("="*50)

try:
    # Step 1: Connect
    conn3 = duckdb.connect("titanic_safe.duckdb")
    print("✅ Connected to Titanic database")
    
    # Step 2: List of Titanic CSV files with meaningful table names
    csv_files = [
        "clean/titanic/titanic.csv", 
        "clean/titanic/titanic_restaraunt.csv", 
        "clean/titanic/titanic_luggage.csv"
    ]
    table_names = ["main_passengers", "dining_records", "luggage_records"]
    
    # Step 3: Create tables one by one
    for i, (csv_file, table_name) in enumerate(zip(csv_files, table_names)):
        try:
            conn3.execute(f"CREATE TABLE {table_name} AS SELECT * FROM '{csv_file}'")
            count = conn3.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            unique_passengers = conn3.execute(f"SELECT COUNT(DISTINCT passenger_id) FROM {table_name}").fetchone()[0]
            print(f"✅ Table {table_name}: {count} rows, {unique_passengers} unique passengers")
        except Exception as e:
            print(f"❌ Error creating table {table_name}: {e}")
    
    # Step 4: Titanic-specific analysis
    print("\nTitanic Database Analysis:")
    
    # Check survival rates
    survival_stats = conn3.execute("""
        SELECT 
            survived,
            COUNT(*) as passenger_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM main_passengers 
        GROUP BY survived
    """).fetchall()
    
    print("Survival Statistics:")
    for stat in survival_stats:
        status = "Survived" if stat[0] == 1 else "Did not survive"
        print(f"  {status}: {stat[1]} passengers ({stat[2]}%)")
    
    # Step 5: Final verification
    final_tables = conn3.execute("SHOW TABLES").fetchall()
    print(f"✅ Final Titanic database contains: {[table[0] for table in final_tables]}")
    
    # Step 6: Close
    conn3.close()
    print("✅ Titanic database saved successfully")
    
except Exception as e:
    print(f"❌ Database error: {e}")

print("\n🚢 All Titanic methods completed! You now have DuckDB files with passenger, restaurant, and luggage data linked by passenger_id.")