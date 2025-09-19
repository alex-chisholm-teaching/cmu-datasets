import pandas as pd
import duckdb
import os

def create_elections_database():
    """
    Creates a DuckDB database file from the CSV files in the elections dataset.
    
    Returns:
        str: Path to the created database file
    """
    # Determine base path depending on current working directory
    if os.path.exists("candidates.csv"):
        # Running from within elections directory
        base_path = "."
    elif os.path.exists("assignments/elections/candidates.csv"):
        # Running from project root
        base_path = "assignments/elections"
    else:
        raise FileNotFoundError("Cannot find election CSV files. Please run from project root or elections directory.")
    
    db_path = os.path.join(base_path, "elections.duckdb")
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Read CSV files with appropriate separators and correct filenames
    print("Loading CSV files...")
    candidates_df = pd.read_csv(os.path.join(base_path, "candidates.csv"), sep='\t')  # Tab-separated
    elections_df = pd.read_csv(os.path.join(base_path, "elections.csv"))
    parties_df = pd.read_csv(os.path.join(base_path, "parties.csv"))  # Corrected filename
    results_df = pd.read_csv(os.path.join(base_path, "results.csv"))  # Corrected filename
    
    # Create DuckDB connection and tables
    print("Creating DuckDB database...")
    conn = duckdb.connect(db_path)
    
    try:
        # Create tables from dataframes
        conn.execute("CREATE TABLE candidates AS SELECT * FROM candidates_df")
        conn.execute("CREATE TABLE elections AS SELECT * FROM elections_df")
        conn.execute("CREATE TABLE political_parties AS SELECT * FROM parties_df")
        conn.execute("CREATE TABLE results AS SELECT * FROM results_df")
        
        # Verify tables were created
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"Created {len(tables)} tables:")
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
            print(f"  - {table[0]}: {count} rows")
        
        print(f"\nDatabase created successfully at: {db_path}")
        return db_path
        
    finally:
        conn.close()

def test_database_connection(db_path=None):
    """
    Test the database connection and show basic information.
    
    Args:
        db_path: Path to the database file. If None, uses default path.
    """
    if db_path is None:
        # Determine database path depending on current working directory
        if os.path.exists("elections.duckdb"):
            db_path = "elections.duckdb"
        elif os.path.exists("assignments/elections/elections.duckdb"):
            db_path = "assignments/elections/elections.duckdb"
        else:
            db_path = "elections.duckdb"  # Default fallback
    
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}. Run create_elections_database() first.")
        return False
    
    print(f"Connecting to database at: {db_path}")
    conn = duckdb.connect(db_path)
    
    try:
        # Test connection with a sample query
        result = conn.execute("""
            SELECT 
                c.name,
                e.election_year,
                er.electoral_votes,
                p.party_name
            FROM candidates c
            JOIN results er ON c.candidate_id = er.candidate_id
            JOIN elections e ON er.election_id = e.election_id
            LEFT JOIN political_parties p ON er.party_id = p.party_id
            WHERE er.result_type = 'winner'
            ORDER BY e.election_year DESC
            LIMIT 5
        """).fetchall()
        
        print("Connection successful! Recent election winners:")
        print("Year | Winner | Electoral Votes | Party")
        print("-" * 50)
        for row in result:
            party = row[3] if row[3] else "Independent"
            print(f"{row[1]} | {row[0]} | {row[2]} | {party}")
        
        return True
        
    except Exception as e:
        print(f"Error testing database: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    # Create the database
    db_path = create_elections_database()
    
    # Test the connection
    test_database_connection(db_path)
