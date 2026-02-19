import duckdb
import os
import re
from datetime import datetime

# Use absolute path so script works regardless of working directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPT_DIR, 'backups')

MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
MOTHERDUCK_DB = 'md:TMDB'

# System tables to exclude from counts
SYSTEM_TABLES = {
    'databases', 'owned_shares', 'shared_with_me',
    'storage_info', 'query_history'
}

# Pattern to detect timestamped backup tables e.g. movies_20260219_163858
TIMESTAMP_PATTERN = re.compile(r'_\d{8}_\d{6}$')


def get_motherduck_connection():
    """Connect to MotherDuck using MOTHERDUCK_TOKEN env var (picked up automatically)."""
    if not MOTHERDUCK_TOKEN:
        raise ValueError("MOTHERDUCK_TOKEN environment variable not set")
    # Token is read automatically from the environment by the MotherDuck extension
    return duckdb.connect(MOTHERDUCK_DB)


def list_backups():
    """List all available backups."""
    if not os.path.exists(BACKUP_DIR):
        print(f"No backup directory found at '{BACKUP_DIR}'")
        return []

    backups = [
        f for f in os.listdir(BACKUP_DIR)
        if os.path.isfile(os.path.join(BACKUP_DIR, f))
        and f.startswith('duckdb_backup_')
        and f.endswith('.db')
    ]

    backups.sort(reverse=True)  # Most recent first

    if not backups:
        print(f"No backups found in '{BACKUP_DIR}'")
        return []

    print(f"\n{'='*60}")
    print("AVAILABLE BACKUPS")
    print(f"{'='*60}")
    for i, backup in enumerate(backups, 1):
        path = os.path.join(BACKUP_DIR, backup)
        size = os.path.getsize(path) / (1024 * 1024)  # MB
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
        key_tables = ['movies', 'tv_shows', 'movie_cast', 'movie_crew', 'tv_show_cast_crew']

        for table_name in key_tables:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"   {table_name}: {count:,} rows")
            except Exception:
                pass  # Table doesn't exist in this backup, skip

        # Test 4: Sample queries
        print("\n4. Running sample queries...")
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

        # Test 5: Column types
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

        # Test 6: Integrity check
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
        print(f"\nThis backup appears valid.")
        print(f"\nTo restore this backup to MotherDuck:")
        print(f"  1. Make sure MOTHERDUCK_TOKEN is set in your environment")
        print(f"  2. Run the following Python:")
        print(f"     conn = duckdb.connect()")
        print(f"     conn.execute(\"ATTACH '{backup_path}' AS backup_db\")")
        print(f"     conn.execute(\"ATTACH 'md:TMDB' AS target\")")
        print(f"     conn.execute('COPY FROM DATABASE backup_db TO target')")
        return True

    except Exception as e:
        print(f"\n✗ BACKUP TEST FAILED")
        print(f"Error: {e}")
        print(f"\nMake sure '{backup_path}' is a valid DuckDB database file.")
        return False


def compare_with_current():
    """Compare a local backup with the live MotherDuck database."""
    print(f"\n{'='*60}")
    print("COMPARING BACKUP WITH LIVE MOTHERDUCK DATABASE")
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

    try:
        print(f"\nConnecting to backup: {backups[idx]}...")
        backup_con = duckdb.connect(database=backup_path, read_only=True)

        print("Connecting to MotherDuck (md:TMDB)...")
        current_con = get_motherduck_connection()

        print(f"\nComparing: {backups[idx]} vs md:TMDB\n")

        backup_tables = set([
            t[0] for t in backup_con.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'main'
            """).fetchall()
            if t[0] not in SYSTEM_TABLES and not TIMESTAMP_PATTERN.search(t[0])
        ])

        current_tables = set([
            t[0] for t in current_con.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'main'
            """).fetchall()
            if t[0] not in SYSTEM_TABLES and not TIMESTAMP_PATTERN.search(t[0])
        ])

        common_tables = backup_tables.intersection(current_tables)

        if common_tables:
            print(f"{'Table':<25} {'Backup':>12} {'MotherDuck':>12} {'Difference':>12}")
            print("-" * 65)

            for table in sorted(common_tables):
                try:
                    backup_count = backup_con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    current_count = current_con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    diff = current_count - backup_count
                    diff_str = f"+{diff}" if diff > 0 else str(diff) if diff < 0 else "0"
                    print(f"{table:<25} {backup_count:>12,} {current_count:>12,} {diff_str:>12}")
                except Exception:
                    print(f"{table:<25} {'ERROR':>12} {'ERROR':>12} {'N/A':>12}")

        backup_only = backup_tables - current_tables
        if backup_only:
            print(f"\n⚠ Tables only in backup: {', '.join(sorted(backup_only))}")

        current_only = current_tables - backup_tables
        if current_only:
            print(f"\n⚠ Tables only in MotherDuck: {', '.join(sorted(current_only))}")

        backup_con.close()
        current_con.close()

    except Exception as e:
        print(f"Error comparing databases: {e}")


def check_motherduck_backup_tables():
    """Inspect live and backup tables in both TMDB and TMDB_backup."""
    print(f"\n{'='*60}")
    print("CHECKING MOTHERDUCK DATABASES")
    print(f"{'='*60}")

    try:
        conn = duckdb.connect()
        conn.execute("ATTACH 'md:TMDB' AS TMDB")
        conn.execute("ATTACH 'md:TMDB_backup' AS TMDB_backup")

        for db_name in ['TMDB', 'TMDB_backup']:
            print(f"\n--- {db_name} ---")

            tables = conn.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                AND table_catalog = '{db_name}'
                ORDER BY table_name
            """).fetchall()

            live = [
                t[0] for t in tables
                if t[0] not in SYSTEM_TABLES
                and not TIMESTAMP_PATTERN.search(t[0])
            ]
            backups = [
                t[0] for t in tables
                if t[0] not in SYSTEM_TABLES
                and TIMESTAMP_PATTERN.search(t[0])
            ]

            # Live tables
            print(f"\n  {'Table':<40} {'Rows':>12}")
            print("  " + "-" * 55)
            for table in live:
                try:
                    count = conn.execute(
                        f'SELECT COUNT(*) FROM {db_name}.main."{table}"'
                    ).fetchone()[0]
                    print(f"  {table:<40} {count:>12,}")
                except Exception:
                    print(f"  {table:<40} {'ERROR':>12}")

            # Timestamped backup tables
            if backups:
                print(f"\n  Timestamped backup tables:")
                for table in backups:
                    try:
                        count = conn.execute(
                            f'SELECT COUNT(*) FROM {db_name}.main."{table}"'
                        ).fetchone()[0]
                        status = "⚠ EMPTY" if count == 0 else "✓"
                        print(f"  {table:<40} {count:>12,}  {status}")
                    except Exception:
                        print(f"  {table:<40} {'ERROR':>12}")
            else:
                print(f"\n  No timestamped backup tables found in {db_name}")

        conn.close()

    except Exception as e:
        print(f"Error: {e}")


# Main menu
if __name__ == "__main__":
    print("=" * 60)
    print("DATABASE BACKUP TESTER")
    print("=" * 60)

    backups = list_backups()

    if not backups:
        print("\nNo backups available to test.")
        print(f"\nMake sure backup files (.db) exist in: {BACKUP_DIR}")
        exit(0)

    print("\nOptions:")
    print("1. Test a specific backup")
    print("2. Compare backup with live MotherDuck database")
    print("3. Check in-DB backup tables in MotherDuck")
    print("4. Exit")

    choice = input("\nEnter choice (1-4): ").strip()

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
        check_motherduck_backup_tables()

    elif choice == '4':
        print("Exiting...")

    else:
        print("Invalid choice")