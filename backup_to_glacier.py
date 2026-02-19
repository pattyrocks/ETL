import os
import sys
import boto3
from datetime import datetime
import duckdb

MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')

def create_sample_database(db_name='TMDB_test.db'):
    conn = duckdb.connect(database=db_name)
    conn.execute('CREATE OR REPLACE TABLE movies (id INTEGER, title VARCHAR, year INTEGER)')
    conn.execute("INSERT INTO movies VALUES (1, 'The Matrix', 1999), (2, 'Inception', 2010), (3, 'Interstellar', 2014)")
    conn.close()
    print(f"Sample database '{db_name}' created with test data.")

def backup_database(use_sample=False):
    try:
        import shutil

        timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        backup_filename = f"duckdb_backup_{timestamp}.db"

        # Use absolute path so it works regardless of working directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        backups_dir = os.path.join(script_dir, 'backups')
        os.makedirs(backups_dir, exist_ok=True)
        backup_file = os.path.join(backups_dir, backup_filename)

        if use_sample:
            print("[Sample Test] Backing up local DuckDB sample database...")
            shutil.copy2('TMDB_test.db', backup_file)
            print(f"Sample backup saved locally as {backup_file}")
            return backup_file

        else:
            print("[Real Run] Backing up MotherDuck (md:TMDB) database using COPY FROM DATABASE...")

            md_timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

            # Delete local file if it already exists from a previous failed run
            if os.path.exists(backup_file):
                os.remove(backup_file)
                print(f"Removed stale local backup file: {backup_file}")

            conn = duckdb.connect()
            conn.execute("ATTACH 'md:TMDB' AS TMDB")
            conn.execute("ATTACH 'md:TMDB_backup' AS TMDB_backup")

            # Find existing backup tables in TMDB_backup (from previous runs)
            print("Finding previous backup tables in TMDB_backup...")
            existing_tables = conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                AND table_catalog = 'TMDB_backup'
            """).fetchall()
            previous_tables = [t[0] for t in existing_tables]
            print(f"  Found {len(previous_tables)} existing tables to replace after new backup completes")

            # Get live tables from TMDB (exclude any stale backup tables)
            source_tables = conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                AND table_catalog = 'TMDB'
                AND table_name NOT LIKE '%\_backup\_%' ESCAPE '\\'
            """).fetchall()

            # Copy each table to TMDB_backup with timestamp suffix
            print(f"Copying tables to TMDB_backup with timestamp {md_timestamp}...")
            new_tables = []
            for table in source_tables:
                name = table[0]
                new_name = f"{name}_{md_timestamp}"
                conn.execute(f"""
                    CREATE OR REPLACE TABLE TMDB_backup.main."{new_name}" AS
                    SELECT * FROM TMDB.main."{name}"
                """)
                new_tables.append(new_name)
                print(f"  ✓ Copied: {name} → {new_name}")

            # Only delete previous tables after all new ones created successfully
            if previous_tables:
                print("Deleting previous backup tables...")
                for table_name in previous_tables:
                    conn.execute(f'DROP TABLE IF EXISTS TMDB_backup.main."{table_name}"')
                    print(f"  Dropped: {table_name}")

            # Copy TMDB to local file for S3 upload table by table
            # Using CREATE OR REPLACE to avoid conflicts on any stale local file
            print("Copying TMDB to local file for S3 upload...")
            conn.execute(f"ATTACH '{backup_file}' AS local_db")
            for table in source_tables:
                name = table[0]
                conn.execute(f"""
                    CREATE OR REPLACE TABLE local_db.main."{name}" AS
                    SELECT * FROM TMDB.main."{name}"
                """)
                print(f"  ✓ Local copy: {name}")
            conn.close()

            print(f"Backup saved locally as {backup_file}")
            return backup_file

    except Exception as e:
        print(f"Error backing up database: {e}")
        exit(1)

def upload_to_s3(backup_file):
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('S3_BUCKET_NAME')

    # Fail immediately with a clear message
    missing = [k for k, v in {
        'AWS_ACCESS_KEY_ID': aws_access_key,
        'AWS_SECRET_ACCESS_KEY': aws_secret_key,
        'S3_BUCKET_NAME': bucket_name
    }.items() if not v]

    if missing:
        print(f"Error: Missing required environment variables: {', '.join(missing)}")
        exit(1)

    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name='ap-south-1'
        )
        s3.upload_file(
            backup_file,
            bucket_name,
            os.path.basename(backup_file),
            ExtraArgs={'StorageClass': 'GLACIER'}
        )
        print(f"Uploaded {os.path.basename(backup_file)} to S3 bucket {bucket_name} with Glacier Flexible Retrieval storage.")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        exit(1)


if __name__ == "__main__":
    use_sample = "--sample" in sys.argv

    if use_sample:
        create_sample_database()

    backup_file = backup_database(use_sample=use_sample)
    upload_to_s3(backup_file)