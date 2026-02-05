import os
import boto3
from datetime import datetime
import duckdb

# Function to create a backup of the DuckDB database
def backup_database():
    try:
        # Define backup filename with timestamp
        timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        backup_file = f"duckdb_backup_{timestamp}.db"
        
        # Connect to MotherDuck (DuckDB) and perform the backup
        conn = duckdb.connect(database='your_database_location.db')  # Specify your database location
        with open(backup_file, 'wb') as f:
            f.write(conn.execute("SELECT * FROM your_table_name").fetchall())
        print(f"Backup saved locally as {backup_file}")
        return backup_file
    
    except Exception as e:
        print(f"Error backing up database: {e}")
        exit(1)

# Function to upload the backup to S3
def upload_to_s3(backup_file):
    try:
        # Gather AWS credentials from environment variables
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        bucket_name = os.getenv('S3_BUCKET_NAME', 'your-default-bucket-name')  # Default bucket name

        # Create S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

        # Create bucket if it does not exist
        if not bucket_exists(s3, bucket_name):
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} created.")

        # Upload the backup file to S3 with Glacier storage class
        s3.upload_file(backup_file, bucket_name, backup_file, ExtraArgs={'StorageClass': 'GLACIER'})
        print(f"Uploaded {backup_file} to S3 bucket {bucket_name} with Glacier storage.")
        
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        exit(1)

# Function to check if an S3 bucket exists
def bucket_exists(s3, bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except Exception:
        return False

if __name__ == "__main__":
    backup_file = backup_database()
    upload_to_s3(backup_file)