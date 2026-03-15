import tmdbsimple as tmdb
import os
import duckdb
import pandas as pd
import time
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from requests.exceptions import HTTPError

# --- Configuration ---
API_KEY = os.getenv('TMDBAPIKEY')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')

# Use MotherDuck if token available, otherwise local
if MOTHERDUCK_TOKEN:
    DATABASE_PATH = f'md:TMDB?motherduck_token={MOTHERDUCK_TOKEN}'
else:
    DATABASE_PATH = 'TMDB'

tmdb.API_KEY = API_KEY

MAX_API_WORKERS = 15
API_BATCH_SIZE = 500  # Report progress every N shows

# --- Logging setup ---
_log_file = f"tv_shows_diagnostics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
    """Log to file and print message to console at the given level."""
    if level == 'debug':
        logger.debug(message)
    elif level == 'warning':
        logger.warning(message)
    elif level == 'error':
        logger.error(message)
    else:
        logger.info(message)


def ensure_tv_shows_table(con):
    """Create or update tv_shows table with all required columns."""
    con.execute('''
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
    ''')
    
    # Add new columns to existing table (idempotent)
    new_columns = [
        ("overview", "VARCHAR"),
        ("poster_path", "VARCHAR"),
        ("backdrop_path", "VARCHAR"),
        ("popularity", "DOUBLE"),
        ("vote_average", "DOUBLE"),
        ("vote_count", "INTEGER"),
        ("first_air_date", "DATE"),
        ("original_language", "VARCHAR"),
        ("original_name", "VARCHAR"),
        ("genres", "VARCHAR"),
        ("networks", "VARCHAR"),
        ("created_by", "VARCHAR"),
        ("tagline", "VARCHAR"),
        ("adult", "BOOLEAN"),
        ("inserted_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
    ]
    
    for col_name, col_type in new_columns:
        try:
            con.execute(f"ALTER TABLE tv_shows ADD COLUMN IF NOT EXISTS {col_name} {col_type};")
        except Exception as e:
            log_and_print(f"Could not add column {col_name}: {e}", level='debug')


def safe_json_str(value):
    """Convert list/dict to string, handling None safely."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return str(value)
    return str(value) if value else None


def safe_list_str(value):
    """Convert list to comma-separated string."""
    if value is None:
        return None
    if isinstance(value, list):
        return ', '.join(str(item) for item in value)
    return str(value) if value else None


def fetch_tv_show_info(tv_show_id):
    """Fetch detailed TV show info from TMDB API with retry logic."""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            tv_show_info = tmdb.TV(tv_show_id).info()
            
            # Extract and normalize fields
            return {
                'id': tv_show_info.get('id'),
                'name': tv_show_info.get('name'),
                'overview': tv_show_info.get('overview'),
                'poster_path': tv_show_info.get('poster_path'),
                'backdrop_path': tv_show_info.get('backdrop_path'),
                'popularity': tv_show_info.get('popularity'),
                'vote_average': tv_show_info.get('vote_average'),
                'vote_count': tv_show_info.get('vote_count'),
                'first_air_date': tv_show_info.get('first_air_date') or None,
                'last_air_date': tv_show_info.get('last_air_date') or None,
                'episode_run_time': safe_list_str(tv_show_info.get('episode_run_time')),
                'homepage': tv_show_info.get('homepage'),
                'in_production': tv_show_info.get('in_production'),
                'number_of_episodes': tv_show_info.get('number_of_episodes'),
                'number_of_seasons': tv_show_info.get('number_of_seasons'),
                'origin_country': safe_list_str(tv_show_info.get('origin_country')),
                'original_language': tv_show_info.get('original_language'),
                'original_name': tv_show_info.get('original_name'),
                'production_countries': safe_json_str(tv_show_info.get('production_countries')),
                'genres': safe_json_str(tv_show_info.get('genres')),
                'networks': safe_json_str(tv_show_info.get('networks')),
                'created_by': safe_json_str(tv_show_info.get('created_by')),
                'status': tv_show_info.get('status'),
                'type': tv_show_info.get('type'),
                'tagline': tv_show_info.get('tagline'),
                'adult': tv_show_info.get('adult'),
            }
            
        except HTTPError as e:
            if e.response.status_code == 404:
                log_and_print(f"TV show {tv_show_id} not found (404)", level='warning')
                return ('not_found', tv_show_id)
            elif e.response.status_code == 429:
                # Rate limited - wait and retry
                wait_time = retry_delay * (attempt + 1)
                log_and_print(f"Rate limited on TV show {tv_show_id}, waiting {wait_time}s", level='warning')
                time.sleep(wait_time)
                continue
            else:
                log_and_print(f"HTTPError for TV show {tv_show_id}: {e}", level='error')
                return ('http', tv_show_id)
        except KeyError as e:
            log_and_print(f"KeyError for TV show {tv_show_id}: {e}", level='error')
            return ('key', tv_show_id)
        except Exception as e:
            log_and_print(f"Unexpected error for TV show {tv_show_id}: {e}", level='error')
            return ('other', tv_show_id)
    
    return ('retry_exhausted', tv_show_id)


def add_info_to_tv_shows_parallel(max_workers=MAX_API_WORKERS):
    """Fetch and update TV show details in parallel."""
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)
    ensure_tv_shows_table(con)
    
    # Get TV shows that need updating (missing key fields)
    tv_show_ids_df = con.execute('''
        SELECT id FROM tv_shows 
        WHERE overview IS NULL 
           OR popularity IS NULL
        ORDER BY id
    ''').fetchdf()
    
    if tv_show_ids_df.empty:
        log_and_print("No TV shows need updating.")
        con.close()
        return
    
    total_to_process = len(tv_show_ids_df)
    log_and_print(f"Starting TV show data retrieval for {total_to_process} shows...")
    
    start_time = time.time()
    processed_count = 0
    skipped_ids = []
    successful_updates = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_tv_show_info, tv_id): tv_id 
            for tv_id in tv_show_ids_df['id']
        }
        
        for future in as_completed(futures):
            tv_id = futures[future]
            result = future.result()
            
            if isinstance(result, tuple):
                # Error occurred
                skipped_ids.append(result[1])
            elif isinstance(result, dict):
                successful_updates.append(result)
            
            processed_count += 1
            
            # Progress logging
            if processed_count % API_BATCH_SIZE == 0 or processed_count == total_to_process:
                elapsed = time.time() - start_time
                rate = processed_count / elapsed if elapsed > 0 else 0
                eta = (total_to_process - processed_count) / rate if rate > 0 else 0
                pct = (processed_count / total_to_process) * 100
                log_and_print(
                    f"Progress: {processed_count}/{total_to_process} ({pct:.1f}%) "
                    f"- {rate:.1f}/s - ETA: {eta:.0f}s"
                )
            
            # Batch insert every 1000 records
            if len(successful_updates) >= 1000:
                _batch_upsert_tv_shows(con, successful_updates)
                successful_updates = []
    
    # Final batch insert
    if successful_updates:
        _batch_upsert_tv_shows(con, successful_updates)
    
    elapsed = time.time() - start_time
    log_and_print(f"\nProcessing complete in {elapsed:.2f}s")
    log_and_print(f"Successfully processed: {processed_count - len(skipped_ids)}")
    
    if skipped_ids:
        log_and_print(f"Skipped {len(skipped_ids)} TV shows due to errors", level='warning')
    
    con.close()


def _batch_upsert_tv_shows(con, records):
    """Batch upsert TV show records."""
    if not records:
        return
    
    df = pd.DataFrame(records)
    con.register('tv_update_df', df)
    
    # Use INSERT OR REPLACE for upsert behavior
    columns = list(records[0].keys())
    columns_str = ', '.join(columns)
    
    con.execute(f"""
        INSERT OR REPLACE INTO tv_shows ({columns_str}, updated_at)
        SELECT {columns_str}, CURRENT_TIMESTAMP
        FROM tv_update_df
    """)
    
    log_and_print(f"Upserted {len(records)} TV show records")


if __name__ == "__main__":
    add_info_to_tv_shows_parallel()
