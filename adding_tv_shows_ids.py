import os
import duckdb
import time
import logging
import gzip
import json
import requests
from datetime import datetime, timedelta

# --- Configuration ---
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')

if not MOTHERDUCK_TOKEN:
    raise EnvironmentError("MOTHERDUCK_TOKEN environment variable is not set")

DATABASE_PATH = f'md:TMDB?motherduck_token={MOTHERDUCK_TOKEN}'
BATCH_SIZE = 5000  # Insert batch size

# --- Logging setup ---
_log_file = f"adding_tv_shows_ids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(_log_file),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


def log_and_print(message, level='info'):
    """Log to file and print message to console."""
    getattr(logger, level)(message)


def ensure_tv_shows_table(con):
    """Ensure tv_shows table exists with correct schema."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS tv_shows (
            id BIGINT PRIMARY KEY,
            name VARCHAR,
            overview VARCHAR,
            poster_path VARCHAR,
            backdrop_path VARCHAR,
            popularity DOUBLE,
            vote_average DOUBLE,
            vote_count INTEGER,
            first_air_date DATE,
            last_air_date DATE,
            episode_run_time VARCHAR,
            homepage VARCHAR,
            in_production BOOLEAN,
            number_of_episodes INTEGER,
            number_of_seasons INTEGER,
            origin_country VARCHAR,
            original_language VARCHAR,
            original_name VARCHAR,
            production_countries VARCHAR,
            genres VARCHAR,
            networks VARCHAR,
            created_by VARCHAR,
            status VARCHAR,
            type VARCHAR,
            tagline VARCHAR,
            adult BOOLEAN,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    log_and_print("tv_shows table ensured.")


def get_existing_tv_ids(con):
    """Get set of existing TV show IDs from database."""
    try:
        result = con.execute("SELECT id FROM tv_shows").fetchdf()
        return set(result['id'].tolist())
    except Exception as e:
        log_and_print(f"Error fetching existing IDs (table may be empty): {e}", level='warning')
        return set()


def download_tmdb_tv_ids_export():
    """
    Download the daily TMDB TV series IDs export file.
    TMDB exports are available at:
    http://files.tmdb.org/p/exports/tv_series_ids_MM_DD_YYYY.json.gz
    
    The export is generated daily at ~7:00 AM UTC, so we try today first,
    then fall back to yesterday if not available yet.
    """
    base_url = "http://files.tmdb.org/p/exports"
    
    # Try today's export first, then yesterday's, then day before
    dates_to_try = [
        datetime.utcnow(),
        datetime.utcnow() - timedelta(days=1),
        datetime.utcnow() - timedelta(days=2),
    ]
    
    for date in dates_to_try:
        filename = f"tv_series_ids_{date.strftime('%m_%d_%Y')}.json.gz"
        url = f"{base_url}/{filename}"
        
        log_and_print(f"Trying to download: {url}")
        
        try:
            response = requests.get(url, timeout=120)
            
            if response.status_code == 200:
                log_and_print(f"Successfully downloaded export: {filename} ({len(response.content) / 1024 / 1024:.2f} MB)")
                return response.content
            elif response.status_code == 404:
                log_and_print(f"Export not found: {filename}, trying older date...")
                continue
            else:
                log_and_print(f"HTTP {response.status_code} for {filename}", level='warning')
                continue
                
        except requests.exceptions.Timeout:
            log_and_print(f"Timeout downloading {filename}", level='warning')
            continue
        except requests.exceptions.RequestException as e:
            log_and_print(f"Error downloading {filename}: {e}", level='error')
            continue
    
    raise Exception("Could not download TMDB TV series IDs export. Check your internet connection.")


def parse_tv_ids_export(gzipped_content):
    """
    Parse the gzipped JSONL export file.
    Each line is a JSON object: {"id": 123, "original_name": "...", "popularity": 1.23}
    """
    all_ids = set()
    
    log_and_print("Decompressing and parsing export file...")
    
    try:
        decompressed = gzip.decompress(gzipped_content)
    except gzip.BadGzipFile as e:
        log_and_print(f"Error decompressing file: {e}", level='error')
        raise
    
    lines = decompressed.decode('utf-8').strip().split('\n')
    
    parse_errors = 0
    for line in lines:
        if not line.strip():  # Skip empty lines
            continue
        try:
            data = json.loads(line)
            tv_id = data.get('id')
            if tv_id is not None:  # Check for None explicitly
                all_ids.add(int(tv_id))  # Ensure integer type
        except json.JSONDecodeError:
            parse_errors += 1
            continue
    
    if parse_errors > 0:
        log_and_print(f"Warning: {parse_errors} lines failed to parse", level='warning')
    
    log_and_print(f"Parsed {len(all_ids)} TV show IDs from export")
    return all_ids


def add_new_tv_show_ids():
    """
    Main function to discover and add new TV show IDs to the database.
    Uses TMDB daily export to get ALL TV IDs, then inserts only the difference.
    """
    log_and_print("=" * 60)
    log_and_print("STARTING TV SHOW ID DISCOVERY (USING TMDB EXPORT)")
    log_and_print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_and_print("=" * 60)
    
    start_time = time.time()
    con = None
    
    try:
        con = duckdb.connect(database=DATABASE_PATH, read_only=False)
    except Exception as e:
        log_and_print(f"Failed to connect to database: {e}", level='error')
        return
    
    try:
        ensure_tv_shows_table(con)
        
        # Get existing IDs from database
        log_and_print("Fetching existing TV show IDs from database...")
        existing_ids = get_existing_tv_ids(con)
        log_and_print(f"Found {len(existing_ids)} existing TV show IDs in database")
        
        # Download and parse TMDB export
        log_and_print("Downloading TMDB TV series IDs export...")
        export_content = download_tmdb_tv_ids_export()
        all_tmdb_ids = parse_tv_ids_export(export_content)
        log_and_print(f"Total TV show IDs in TMDB: {len(all_tmdb_ids)}")
        
        # Calculate difference
        new_ids = all_tmdb_ids - existing_ids
        log_and_print(f"New IDs to add: {len(new_ids)}")
        
        if not new_ids:
            log_and_print("No new TV show IDs to add. Database is up to date!")
            return
        
        # Insert new IDs in batches
        log_and_print(f"Inserting {len(new_ids)} new TV show IDs...")
        
        new_ids_list = list(new_ids)
        inserted_count = 0
        failed_count = 0
        
        for i in range(0, len(new_ids_list), BATCH_SIZE):
            batch = new_ids_list[i:i + BATCH_SIZE]
            
            # Batch insert using VALUES list
            values_str = ', '.join([f"({tid})" for tid in batch])
            try:
                con.execute(f"""
                    INSERT INTO tv_shows (id) 
                    VALUES {values_str}
                    ON CONFLICT (id) DO NOTHING
                """)
                inserted_count += len(batch)
            except Exception as e:
                log_and_print(f"Error inserting batch at index {i}: {e}", level='error')
                # Fall back to individual inserts
                for tv_id in batch:
                    try:
                        con.execute("INSERT INTO tv_shows (id) VALUES (?) ON CONFLICT (id) DO NOTHING", [tv_id])
                        inserted_count += 1
                    except Exception as e2:
                        log_and_print(f"Error inserting ID {tv_id}: {e2}", level='error')
                        failed_count += 1
            
            progress = min(i + BATCH_SIZE, len(new_ids_list))
            percent = (progress / len(new_ids_list)) * 100
            log_and_print(f"Progress: {progress}/{len(new_ids_list)} ({percent:.1f}%)")
        
        log_and_print(f"Successfully inserted {inserted_count} new TV show IDs")
        if failed_count > 0:
            log_and_print(f"Failed to insert {failed_count} IDs", level='warning')
        
    except Exception as e:
        log_and_print(f"Critical error: {e}", level='error')
        import traceback
        log_and_print(traceback.format_exc(), level='error')
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    
    total_elapsed = time.time() - start_time
    minutes, seconds = divmod(total_elapsed, 60)
    
    log_and_print("=" * 60)
    log_and_print(f"TV SHOW ID DISCOVERY COMPLETE in {int(minutes)}m {seconds:.2f}s")
    log_and_print("=" * 60)


if __name__ == "__main__":
    add_new_tv_show_ids()
