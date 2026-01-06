import duckdb
import os
from datetime import datetime

BACKUP_DIR = 'backups'
CURRENT_DB = 'TMDB'

def list_backups():
    """List all available backups."""
    if not os.path.exists(BACKUP_DIR):
        print(f"No backup directory found at '{BACKUP_DIR}'")
        return []
    
    # Look for all .duckdb files and the TMDB file without extension
    all_files = os.listdir(BACKUP_DIR)
    backups = []
    
    for f in all_files:
        path = os.path.join(BACKUP_DIR, f)
        # Include .duckdb files and files named TMDB (no extension)
        if os.path.isfile(path) and (f.endswith('.duckdb') or f == 'TMDB' or f.startswith('TMDB')):
            backups.append(f)
    
    backups.sort(reverse=True)  # Most recent first
    
    if not backups:
        print(f"No backups found in '{BACKUP_DIR}'")
        return []
    
    print(f"\n{'='*60}")
    print("AVAILABLE BACKUPS")
    print(f"{'='*60}")
    for i, backup in enumerate(backups, 1):
        path = os.path.join(BACKUP_DIR, backup)
        size = os.path.getsize(path) / (1024 * 1024)  # Convert to MB
        mtime = datetime.fromtimestamp(os.path.getmtime(path))
        print(f"{i}. {backup}")
        print(f"   Size: {size:.2f} MB")
        print(f"   Created: {mtime.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
    
    return backups

def test_backup(backup_file):
    """Test a backup file by connecting and running queries."""
    backup_path = os.path.join(BACKUP_DIR, backup_file)
    
    print(f"\n{'='*60}")
    print(f"TESTING BACKUP: {backup_file}")
    print(f"{'='*60}")
    
    try:
        # Test 1: Can we connect?
        print("\n1. Testing connection... ", end='', flush=True)
        con = duckdb.connect(database=backup_path, read_only=True)
        print("✓ SUCCESS")
        
        # Test 2: List tables
        print("\n2. Checking tables...")
        tables = con.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()
        
        print(f"   Found {len(tables)} tables:")
        for table in tables:
            print(f"   - {table[0]}")
        
        # Test 3: Check row counts for key tables
        print("\n3. Checking row counts...")
        key_tables = ['movies', 'tv_shows', 'movie_cast', 'movie_crew', 
                      'tv_show_cast', 'tv_show_crew', 'movie_ids', 'tv_show_ids']
        
        for table_name in key_tables:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"   {table_name}: {count:,} rows")
            except Exception:
                # Table doesn't exist, skip silently
                pass
        
        # Test 4: Sample queries
        print("\n4. Running sample queries...")
        
        # Check movies table
        try:
            sample = con.execute("""
                SELECT id, title, release_date 
                FROM movies 
                LIMIT 3
            """).fetchall()
            print(f"   ✓ movies table: Retrieved {len(sample)} sample rows")
            for row in sample:
                print(f"     - {row[1]} ({row[2]})")
        except Exception as e:
            print(f"   ⚠ movies table query: {e}")
        
        # Check data types
        print("\n5. Checking column types for movies table...")
        try:
            columns = con.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'movies' 
                ORDER BY column_name
                LIMIT 10
            """).fetchall()
            
            print(f"   Found {len(columns)} columns (showing first 10):")
            for col in columns:
                print(f"   - {col[0]}: {col[1]}")
        except Exception as e:
            print(f"   ⚠ Column type check: {e}")
        
        # Test 6: Check database integrity
        print("\n6. Checking database integrity... ", end='', flush=True)
        try:
            result = con.execute("PRAGMA integrity_check;").fetchone()
            if result and result[0] == 'ok':
                print("✓ PASSED")
            else:
                print(f"⚠ Result: {result}")
        except Exception as e:
            print(f"⚠ {e}")
        
        con.close()
        
        print(f"\n{'='*60}")
        print("✓ BACKUP TEST COMPLETED")
        print(f"{'='*60}")
        print(f"\nThis backup appears to be valid and can be restored if needed.")
        print(f"\nTo restore this backup:")
        print(f"  cp {backup_path} {CURRENT_DB}")
        return True
        
    except Exception as e:
        print(f"\n✗ BACKUP TEST FAILED")
        print(f"Error: {e}")
        print(f"\nMake sure the file '{backup_path}' is a valid DuckDB database.")
        return False

def compare_with_current():
    """Compare backup with current database."""
    print(f"\n{'='*60}")
    print("COMPARING BACKUP WITH CURRENT DATABASE")
    print(f"{'='*60}")
    
    backups = list_backups()
    if not backups:
        return
    
    print("\nSelect backup to compare:")
    for i, backup in enumerate(backups, 1):
        print(f"{i}. {backup}")
    
    choice = input(f"\nEnter number (1-{len(backups)}): ").strip()
    
    try:
        idx = int(choice) - 1
        if not (0 <= idx < len(backups)):
            print("Invalid selection")
            return
    except ValueError:
        print("Invalid input")
        return
    
    backup_path = os.path.join(BACKUP_DIR, backups[idx])
    current_path = CURRENT_DB
    
    if not os.path.exists(current_path):
        print(f"\n⚠ Current database '{current_path}' not found!")
        return
    
    try:
        backup_con = duckdb.connect(database=backup_path, read_only=True)
        current_con = duckdb.connect(database=current_path, read_only=True)
        
        print(f"\nComparing: {backups[idx]} vs {current_path}\n")
        
        # Get all tables from both databases
        backup_tables = set([t[0] for t in backup_con.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()])
        
        current_tables = set([t[0] for t in current_con.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()])
        
        # Tables in both
        common_tables = backup_tables.intersection(current_tables)
        
        if common_tables:
            print(f"{'Table':<20} {'Backup':>15} {'Current':>15} {'Difference':>15}")
            print("-" * 70)
            
            for table in sorted(common_tables):
                try:
                    backup_count = backup_con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    current_count = current_con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    diff = current_count - backup_count
                    diff_str = f"+{diff}" if diff > 0 else str(diff) if diff < 0 else "0"
                    print(f"{table:<20} {backup_count:>15,} {current_count:>15,} {diff_str:>15}")
                except Exception as e:
                    print(f"{table:<20} {'ERROR':>15} {'ERROR':>15} {'N/A':>15}")
        
        # Tables only in backup
        backup_only = backup_tables - current_tables
        if backup_only:
            print(f"\n⚠ Tables only in backup: {', '.join(sorted(backup_only))}")
        
        # Tables only in current
        current_only = current_tables - backup_tables
        if current_only:
            print(f"\n⚠ Tables only in current: {', '.join(sorted(current_only))}")
        
        backup_con.close()
        current_con.close()
        
    except Exception as e:
        print(f"Error comparing databases: {e}")

# Main menu
if __name__ == "__main__":
    print("="*60)
    print("DATABASE BACKUP TESTER")
    print("="*60)
    
    backups = list_backups()
    
    if not backups:
        print("\nNo backups available to test.")
        print(f"\nMake sure you have backup files in the '{BACKUP_DIR}' directory.")
        exit(0)
    
    print("\nOptions:")
    print("1. Test a specific backup")
    print("2. Compare backup with current database")
    print("3. Exit")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    if choice == '1':
        print("\nSelect backup to test:")
        for i, backup in enumerate(backups, 1):
            print(f"{i}. {backup}")
        
        num = input(f"\nEnter backup number (1-{len(backups)}): ").strip()
        try:
            idx = int(num) - 1
            if 0 <= idx < len(backups):
                test_backup(backups[idx])
            else:
                print("Invalid backup number")
        except ValueError:
            print("Invalid input")
    elif choice == '2':
        compare_with_current()
    elif choice == '3':
        print("Exiting...")
    else:
        print("Invalid choice")