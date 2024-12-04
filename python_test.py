import duckdb




# Connect to the DuckDB database file
conn = duckdb.connect('C:/Users/carls/Documents/data_transformation/wolt_store_model/shared/db/datamart.duckdb')

# Close the existing connection
conn.close()

# Connect to the DuckDB database file
conn = duckdb.connect('C:/Users/carls/Documents/data_transformation/wolt_store_model/shared/db/datamart.duckdb')

try:
    # Alternative method to list tables using information_schema
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'wolt_data_model_core'").fetchall()
    
    # Print the tables
    print("Tables in the database:")
    if tables:
        for table in tables:
            print(f"- {table[0]}")
    else:
        print("No tables found.")

    # Example query: Select all from a specific table
    query = """
SELECT order_id, COUNT(DISTINCT item_id) items
FROM wolt_data_model_core.fact_order_item
GROUP BY order_id
ORDER BY items DESC
LIMIT 100

"""

    
    results = conn.execute(query).fetchall()
    
    # Print the query results
    print("\nQuery results:")
    for row in results:
        print(row)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the connection
    conn.close()