import os
import sys
import boto3
from datetime import datetime
import duckdb

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
        backup_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), backup_filename)

        # Save to ETL/backups folder
        script_dir = os.path.dirname(os.path.abspath(__file__))
        backups_dir = os.path.join(script_dir, 'backups')
        os.makedirs(backups_dir, exist_ok=True)  # Create folder if it doesn't exist
        backup_file = os.path.join(backups_dir, backup_filename)

        if use_sample:
            print("[Sample Test] Backing up local DuckDB sample database...")
            shutil.copy2('TMDB_test.db', backup_file)
            print(f"Sample backup saved locally as {backup_file}")
            return backup_file
        else:
            print("[Real Run] Backing up MotherDuck (md:TMDB) database using COPY FROM DATABASE...")
            conn = duckdb.connect()
            conn.execute("ATTACH 'md:TMDB' AS source")
            conn.execute("ATTACH 'md:TMDB_backup' AS backup_db")
            conn.execute("COPY FROM DATABASE source TO backup_db")
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

    # Fail immediately with clear message instead of cryptic NoneType error
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
            aws_secret_access_key=aws_secret_key
        )
        s3.upload_file(backup_file, bucket_name, os.path.basename(backup_file), 
                      ExtraArgs={'StorageClass': 'DEEP_ARCHIVE'})
        print(f"Uploaded {backup_file} to S3 bucket {bucket_name} with Glacier Deep Archive storage.")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        exit(1)
        try:
            # Gather AWS credentials from environment variables
            aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            bucket_name = os.getenv('S3_BUCKET_NAME')

            # Create S3 client
            s3 = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )

            # Delete prior local backups (except the current one)
            for fname in os.listdir('.'):
                if fname.startswith('duckdb_backup_') and fname.endswith('.db') and fname != backup_file:
                    try:
                        os.remove(fname)
                        print(f"Deleted old local backup: {fname}")
                    except Exception as e:
                        print(f"Failed to delete local backup {fname}: {e}")

            # Delete prior S3 backups (except the current one)
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix='duckdb_backup_')
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key != backup_file and key.endswith('.db'):
                        try:
                            s3.delete_object(Bucket=bucket_name, Key=key)
                            print(f"Deleted old S3 backup: {key}")
                        except Exception as e:
                            print(f"Failed to delete S3 backup {key}: {e}")

            # Upload the backup file to S3 with Glacier storage class
            s3.upload_file(backup_file, bucket_name, backup_file, ExtraArgs={'StorageClass': 'DEEP_ARCHIVE'})
            print(f"Uploaded {backup_file} to S3 bucket {bucket_name} with Glacier Deep Archive storage.")
        
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            exit(1)


if __name__ == "__main__":
    use_sample = "--sample" in sys.argv
    
    if use_sample:
        create_sample_database()
    
    backup_file = backup_database(use_sample=use_sample)
    upload_to_s3(backup_file)