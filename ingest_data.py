"""
Trail Trekker Data Pipepline
"""

import duckdb

def create_features_table(conn):
    """Create features table from CSV using DuckDB's automatic CSV parsing"""
    print("Creating features table...")

    # Use DuckDB's automatic CSV parsing
    conn.execute("""
        CREATE OR REPLACE TABLE features AS 
        SELECT * FROM 'data/features.csv'
""")    
    
    # Validate the table creation
    count = conn.execute("SELECT COUNT(*) FROM features").fetchone()[0]
    print(f"Features table created with {count} rows")

    # Check data types
    print("Data types:")
    schema = conn.execute("DESCRIBE TABLE features").fetchall()
    for column in schema:
        print(f"{column[0]}: {column[1]}")
    
    return True

def create_plans_table(conn):
    """Create plans table with data cleaning"""
    print("Creating plans table...")

    # Create raw plans table
    conn.execute("""
        CREATE OR REPLACE TABLE plans_raw AS
        SELECT * FROM 'data/plans.csv'
""")
    
    # Clean data - remove the NULL row
    conn.execute("""
        CREATE OR REPLACE TABLE plans AS
        SELECT * FROM plans_raw
        WHERE plan_id != '000000' AND plan_id IS NOT NULL
""")
    
    # Drop the raw table
    conn.execute("DROP TABLE plans_raw")
    
    # Validate
    count = conn.execute("SELECT COUNT(*) FROM plans").fetchone()[0]
    print(f"Plans table created with {count} rows (cleaned)")

    # Check the plan levels
    levels = conn.execute("SELECT DISTINCT plan_level FROM plans ORDER BY plan_level").fetchall()
    print(f"Plan levels: {[level[0] for level in levels]}")

    return True

def create_customers_table(conn):
    """Create customers table from CSV"""
    print("Creating customers table...")

    conn.execute("""
        CREATE OR REPLACE TABLE customers AS
        SELECT * FROM 'data/customers.csv'
    """)

    # Validate
    count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    print(f"Customers table created with {count} rows")

    # Check for unique customers
    unique_count = conn.execute("SELECT COUNT(DISTINCT customer_id) FROM customers").fetchone()[0]
    print(f"Unique customers: {unique_count}")

    # Check difficulty levels
    difficulties = conn.execute("SELECT DISTINCT preferred_difficulty FROM customers ORDER BY preferred_difficulty").fetchall()
    print(f"Difficulty levels: {[difficulty[0] for difficulty in difficulties]}")

    return True

def create_plan_features_table(conn):
    """Create plan_features junction table"""
    print("Creating plan_features table...")

    conn.execute("""
        CREATE OR REPLACE TABLE plan_features AS 
        SELECT * FROM 'data/plan_features.csv'
    """)
    
    # Simple validation
    count = conn.execute("SELECT COUNT(*) FROM plan_features").fetchone()[0]
    print(f"Plan_features table created with {count} rows")
    
    return True

def create_subscriptions_table(conn):
    """Create subscriptions table"""
    print("Creating subscriptions table...")

    conn.execute("""
        CREATE OR REPLACE TABLE subscriptions AS 
        SELECT * FROM 'data/subscriptions.csv'
    """)
    
    # Simple validation
    count = conn.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0]
    print(f"Subscriptions table created with {count} rows")
    
    return True

def main():
    print("Starting Trail Trekker Data Pipeline")

    # Create persistent DuckDB database
    conn = duckdb.connect('trail_trekker.db')
    print("Connected to DuckDB database")

    # Check what tables exist (should be empty during first run)
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f"Current tables: {len(tables)}")

    # Create features table
    create_features_table(conn)

    # Create plans table
    create_plans_table(conn)

    # Create customers table
    create_customers_table(conn)

    # Create plan_features_table
    create_plan_features_table(conn)
    
    # Create subscriptions table
    create_subscriptions_table(conn)

    # Check tables again
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f"Tables after creation: {len(tables)}")
    for table in tables:
        print(f" - {table[0]}")

    print("Database setup complete!")
    conn.close()

if __name__ == "__main__":
    main()

