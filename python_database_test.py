import os
import duckdb

# Path to the datamart.duckdb file
db_path = 'C:/Users/carls/Documents/data_transformation/wolt_store_model/shared/db/datamart.duckdb'

# Check if the database file exists
if os.path.exists(db_path):
    # Delete the existing database file
    os.remove(db_path)
    print(f"Deleted existing database at {db_path}")

# Create a new database connection with the same file name
conn = duckdb.connect(db_path)
print(f"Created a new database at {db_path}")

# ... existing code ...

