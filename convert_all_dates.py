import duckdb
import shutil
from datetime import datetime
import os

# Backup settings
DATABASE_PATH = 'TMDB'
BACKUP_DIR = 'backups'

def create_backup():
    """
    Creates a backup of the database before making changes.
    Returns the backup file path if successful, None otherwise.
    """
    try:
        # Create backups directory if it doesn't exist
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
            print(f"Created backup directory: {BACKUP_DIR}")
        
        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = os.path.join(BACKUP_DIR, f'TMDB_backup_{timestamp}.duckdb')
        
        print(f"\n{'='*60}")
        print("CREATING BACKUP")
        print(f"{'='*60}")
        print(f"Source: {DATABASE_PATH}")
        print(f"Backup: {backup_path}")
        print("Creating backup... ", end='', flush=True)
        
        # Copy the database file
        shutil.copy2(DATABASE_PATH, backup_path)
        
        # Verify backup
        backup_size = os.path.getsize(backup_path)
        original_size = os.path.getsize(DATABASE_PATH)
        
        if backup_size == original_size:
            print("✓ SUCCESS")
            print(f"Backup size: {backup_size:,} bytes")
            return backup_path
        else:
            print("✗ FAILED - Size mismatch")
            return None
            
    except Exception as e:
        print(f"✗ FAILED - {e}")
        return None

def convert_column_to_date(con, table_name, column_name):
    """
    Safely converts a VARCHAR column to DATE type.
    Returns True if successful, False otherwise.
    """
    try:
        print(f"\n{'='*60}")
        print(f"Converting {table_name}.{column_name} from VARCHAR to DATE...")
        print(f"{'='*60}")
        
        # Step 1: Create a new column with DATE type
        print(f"1. Adding new DATE column {column_name}_new...")
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name}_new DATE;")
        
        # Step 2: Convert and copy data to new column
        print(f"2. Converting data (this may take a moment)...")
        con.execute(f"""
            UPDATE {table_name} 
            SET {column_name}_new = TRY_CAST({column_name} AS DATE)
            WHERE {column_name} IS NOT NULL
        """)
        
        # Step 3: Check conversion results
        print(f"3. Verifying conversion...")
        stats = con.execute(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT({column_name}) as had_varchar_date,
                COUNT({column_name}_new) as converted_to_date
            FROM {table_name}
        """).fetchone()
        
        print(f"   Total rows: {stats[0]:,}")
        print(f"   Had VARCHAR date: {stats[1]:,}")
        print(f"   Converted to DATE: {stats[2]:,}")
        
        if stats[1] == stats[2]:
            # Step 4: All converted successfully - replace old column
            print(f"\n4. All dates converted successfully! Replacing column...")
            con.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name};")
            con.execute(f"ALTER TABLE {table_name} RENAME COLUMN {column_name}_new TO {column_name};")
            print(f"✓ Conversion complete for {table_name}.{column_name}!")
            return True
        else:
            failed = stats[1] - stats[2]
            print(f"\n⚠ Warning: {failed} dates failed to convert in {table_name}.{column_name}.")
            print(f"Column kept as '{column_name}', new as '{column_name}_new'")
            return False
            
    except Exception as e:
        print(f"\n✗ Error during conversion of {table_name}.{column_name}: {e}")
        return False

# Main execution
print("="*60)
print("DATABASE DATE COLUMN CONVERSION SCRIPT")
print("="*60)

# Step 1: Create backup
backup_path = create_backup()

if not backup_path:
    print("\n✗ BACKUP FAILED - Aborting conversion for safety!")
    print("Please check the error and try again.")
    exit(1)

print(f"\n✓ Backup created successfully: {backup_path}")
print("\nIf anything goes wrong, you can restore from this backup:")
print(f"  cp {backup_path} {DATABASE_PATH}")

# Ask for confirmation
print("\n" + "="*60)
response = input("Continue with conversion? (yes/no): ").strip().lower()
if response not in ['yes', 'y']:
    print("Conversion cancelled by user.")
    exit(0)

# Step 2: Connect to database
con = duckdb.connect(database=DATABASE_PATH, read_only=False)

# List of tables and columns to convert
conversions = [
    ('movies', 'release_date'),      # From movies.py and adding_movies_ids.py
    ('tv_shows', 'last_air_date'),   # From tv_shows.py
]

print("\nStarting batch conversion of VARCHAR date columns to DATE type...")
print(f"Found {len(conversions)} columns to convert\n")

results = []
import time
start_time = time.time()

for table_name, column_name in conversions:
    # Check if table exists
    table_check = con.execute(f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    """).fetchone()
    
    if table_check[0] == 0:
        print(f"\n⚠ Table '{table_name}' does not exist. Skipping.")
        results.append((table_name, column_name, 'SKIPPED - Table not found'))
        continue
    
    # Check if column exists
    column_check = con.execute(f"""
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' 
        AND column_name = '{column_name}'
    """).fetchone()
    
    if column_check[0] == 0:
        print(f"\n⚠ Column '{column_name}' does not exist in '{table_name}'. Skipping.")
        results.append((table_name, column_name, 'SKIPPED - Column not found'))
        continue
    
    # Perform conversion
    success = convert_column_to_date(con, table_name, column_name)
    results.append((table_name, column_name, 'SUCCESS' if success else 'FAILED'))

end_time = time.time()
elapsed = end_time - start_time

# Close connection
con.close()

# Print summary
print(f"\n{'='*60}")
print("CONVERSION SUMMARY")
print(f"{'='*60}")
print(f"Total time elapsed: {elapsed:.2f} seconds")
print(f"Backup location: {backup_path}\n")

for table, column, status in results:
    status_symbol = "✓" if status == "SUCCESS" else "⚠" if "SKIPPED" in status else "✗"
    print(f"{status_symbol} {table}.{column}: {status}")

print("\n" + "="*60)
print("NEXT STEPS")
print("="*60)
print("Remember to update your Python ETL scripts:")
print("1. movies.py: Change 'release_date VARCHAR' to 'release_date DATE'")
print("2. tv_shows.py: Change 'last_air_date VARCHAR' to 'last_air_date DATE'")
print("3. adding_movies_ids.py: Update INSERT to cast date properly")
print("4. adding_tv_shows_ids.py: Update INSERT to cast date properly")
print("\nIf you need to restore from backup:")
print(f"  cp {backup_path} {DATABASE_PATH}")
print("="*60)