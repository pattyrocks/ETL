import tmdbsimple as tmdb
import os
import time
import duckdb
import pandas as pd
import logging
import math
import pickle
import gzip
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from requests.exceptions import HTTPError

# --- Configuration ---
API_KEY = os.getenv('TMDBAPIKEY')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')

if not API_KEY:
    raise EnvironmentError("TMDBAPIKEY environment variable is not set")
if not MOTHERDUCK_TOKEN:
    raise EnvironmentError("MOTHERDUCK_TOKEN environment variable is not set")

DATABASE_PATH = f'md:TMDB?motherduck_token={MOTHERDUCK_TOKEN}'
tmdb.API_KEY = API_KEY

MAX_API_WORKERS = 15
DB_INSERT_BATCH_SIZE = 5000
API_BATCH_SIZE = 500
MAX_RETRIES = 3
RATE_LIMIT_RETRY_DELAY = 2

# --- Logging setup ---
_log_file = f"update_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(_log_file),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def log_and_print(message, level='info'):
    """Log to file and print message to console."""
    getattr(logger, level)(message)


def handle_rate_limit(attempt):
    """Handle rate limiting with exponential backoff."""
    wait_time = RATE_LIMIT_RETRY_DELAY * (2 ** attempt)
    log_and_print(f"Rate limited. Waiting {wait_time}s before retry...", level='warning')
    time.sleep(wait_time)


def save_checkpoint(processed_ids, filename):
    """Save processed IDs to a checkpoint file."""
    try:
        with open(filename, 'wb') as f:
            pickle.dump(processed_ids, f)
    except IOError as e:
        log_and_print(f"Failed to save checkpoint {filename}: {e}", level='error')


def load_checkpoint(filename):
    """Load processed IDs from a checkpoint file."""
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return set()
    except Exception as e:
        log_and_print(f"Failed to load checkpoint {filename}: {e}", level='warning')
        return set()


def log_null_columns(df, log_file):
    """Log columns with null values."""
    if df.empty:
        return
    null_counts = df.isnull().sum()
    try:
        with open(log_file, 'w') as f:
            for col, count in null_counts.items():
                if count > 0:
                    f.write(f"{col}: {count} nulls\n")
        log_and_print(f"Null column log written to {log_file}")
    except IOError as e:
        log_and_print(f"Failed to write null column log: {e}", level='error')


def log_skipped_ids(skipped_ids, filename):
    """Write skipped IDs to a log file."""
    if not skipped_ids:
        return
    try:
        with open(filename, 'w') as f:
            for sid in skipped_ids:
                f.write(f"{sid}\n")
        log_and_print(f"Wrote {len(skipped_ids)} skipped IDs to {filename}", level='warning')
    except IOError as e:
        log_and_print(f"Failed to write skipped IDs log: {e}", level='error')


def safe_str(value):
    """Safely convert value to string, handling lists/dicts."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return str(value)
    return str(value) if value else None


# =============================================================================
# ID DISCOVERY FUNCTIONS (TMDB EXPORTS)
# =============================================================================

def download_tmdb_export(export_type):
    """
    Download the daily TMDB export file.
    export_type: 'movie_ids' or 'tv_series_ids'
    """
    base_url = "http://files.tmdb.org/p/exports"
    
    dates_to_try = [
        datetime.utcnow(),
        datetime.utcnow() - timedelta(days=1),
        datetime.utcnow() - timedelta(days=2),
    ]
    
    for date in dates_to_try:
        filename = f"{export_type}_{date.strftime('%m_%d_%Y')}.json.gz"
        url = f"{base_url}/{filename}"
        
        log_and_print(f"Trying to download: {url}")
        
        try:
            response = requests.get(url, timeout=120)
            
            if response.status_code == 200:
                log_and_print(f"Successfully downloaded: {filename} ({len(response.content) / 1024 / 1024:.2f} MB)")
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
    
    raise Exception(f"Could not download TMDB {export_type} export")


def parse_ids_export(gzipped_content):
    """Parse the gzipped JSONL export file."""
    all_ids = set()
    
    try:
        decompressed = gzip.decompress(gzipped_content)
    except gzip.BadGzipFile as e:
        log_and_print(f"Error decompressing file: {e}", level='error')
        raise
    
    lines = decompressed.decode('utf-8').strip().split('\n')
    
    for line in lines:
        if not line.strip():
            continue
        try:
            data = json.loads(line)
            item_id = data.get('id')
            if item_id is not None:
                all_ids.add(int(item_id))
        except json.JSONDecodeError:
            continue
    
    return all_ids


def ensure_movies_table(con):
    """Ensure movies table exists with correct schema."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id BIGINT PRIMARY KEY,
            adult BOOLEAN,
            backdrop_path VARCHAR,
            belongs_to_collection VARCHAR,
            budget BIGINT,
            genres VARCHAR,
            homepage VARCHAR,
            imdb_id VARCHAR,
            origin_country VARCHAR[],
            original_language VARCHAR,
            original_title VARCHAR,
            overview VARCHAR,
            popularity DOUBLE,
            poster_path VARCHAR,
            production_companies VARCHAR,
            production_countries VARCHAR,
            release_date DATE,
            revenue BIGINT,
            runtime INTEGER,
            spoken_languages VARCHAR,
            status VARCHAR,
            tagline VARCHAR,
            title VARCHAR,
            video BOOLEAN,
            vote_average DOUBLE,
            vote_count INTEGER,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


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


def ensure_cast_crew_tables(con):
    """Ensure cast/crew tables exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS tv_show_cast_crew (
            tv_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            character VARCHAR,
            cast_order INTEGER,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            roles VARCHAR,
            total_episode_count INTEGER,
            cast_id BIGINT,
            also_known_as VARCHAR,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS movie_cast (
            movie_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            character VARCHAR,
            cast_order INTEGER,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            cast_id BIGINT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS movie_crew (
            movie_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            adult BOOLEAN,
            department VARCHAR,
            job VARCHAR,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def discover_new_movie_ids(con):
    """Discover and add new movie IDs from TMDB export."""
    log_and_print("=" * 60)
    log_and_print("DISCOVERING NEW MOVIE IDS")
    log_and_print("=" * 60)
    
    start_time = time.time()
    
    try:
        ensure_movies_table(con)
        
        # Get existing IDs
        log_and_print("Fetching existing movie IDs from database...")
        result = con.execute("SELECT id FROM movies").fetchdf()
        existing_ids = set(result['id'].tolist()) if not result.empty else set()
        log_and_print(f"Found {len(existing_ids)} existing movie IDs")
        
        # Download and parse export
        log_and_print("Downloading TMDB movie IDs export...")
        export_content = download_tmdb_export('movie_ids')
        all_tmdb_ids = parse_ids_export(export_content)
        log_and_print(f"Total movie IDs in TMDB: {len(all_tmdb_ids)}")
        
        # Calculate difference
        new_ids = all_tmdb_ids - existing_ids
        log_and_print(f"New movie IDs to add: {len(new_ids)}")
        
        if not new_ids:
            log_and_print("No new movie IDs to add.")
            return
        
        # Insert new IDs in batches
        new_ids_list = list(new_ids)
        inserted_count = 0
        
        for i in range(0, len(new_ids_list), DB_INSERT_BATCH_SIZE):
            batch = new_ids_list[i:i + DB_INSERT_BATCH_SIZE]
            values_str = ', '.join([f"({mid})" for mid in batch])
            try:
                con.execute(f"INSERT INTO movies (id) VALUES {values_str} ON CONFLICT (id) DO NOTHING")
                inserted_count += len(batch)
            except Exception as e:
                log_and_print(f"Error inserting movie batch: {e}", level='error')
            
            progress = min(i + DB_INSERT_BATCH_SIZE, len(new_ids_list))
            log_and_print(f"Movie ID Progress: {progress}/{len(new_ids_list)}")
        
        elapsed = time.time() - start_time
        log_and_print(f"Inserted {inserted_count} new movie IDs in {elapsed:.2f}s")
        
    except Exception as e:
        log_and_print(f"Error discovering movie IDs: {e}", level='error')


def discover_new_tv_show_ids(con):
    """Discover and add new TV show IDs from TMDB export."""
    log_and_print("=" * 60)
    log_and_print("DISCOVERING NEW TV SHOW IDS")
    log_and_print("=" * 60)
    
    start_time = time.time()
    
    try:
        ensure_tv_shows_table(con)
        
        # Get existing IDs
        log_and_print("Fetching existing TV show IDs from database...")
        result = con.execute("SELECT id FROM tv_shows").fetchdf()
        existing_ids = set(result['id'].tolist()) if not result.empty else set()
        log_and_print(f"Found {len(existing_ids)} existing TV show IDs")
        
        # Download and parse export
        log_and_print("Downloading TMDB TV series IDs export...")
        export_content = download_tmdb_export('tv_series_ids')
        all_tmdb_ids = parse_ids_export(export_content)
        log_and_print(f"Total TV show IDs in TMDB: {len(all_tmdb_ids)}")
        
        # Calculate difference
        new_ids = all_tmdb_ids - existing_ids
        log_and_print(f"New TV show IDs to add: {len(new_ids)}")
        
        if not new_ids:
            log_and_print("No new TV show IDs to add.")
            return
        
        # Insert new IDs in batches
        new_ids_list = list(new_ids)
        inserted_count = 0
        
        for i in range(0, len(new_ids_list), DB_INSERT_BATCH_SIZE):
            batch = new_ids_list[i:i + DB_INSERT_BATCH_SIZE]
            values_str = ', '.join([f"({tid})" for tid in batch])
            try:
                con.execute(f"INSERT INTO tv_shows (id) VALUES {values_str} ON CONFLICT (id) DO NOTHING")
                inserted_count += len(batch)
            except Exception as e:
                log_and_print(f"Error inserting TV show batch: {e}", level='error')
            
            progress = min(i + DB_INSERT_BATCH_SIZE, len(new_ids_list))
            log_and_print(f"TV Show ID Progress: {progress}/{len(new_ids_list)}")
        
        elapsed = time.time() - start_time
        log_and_print(f"Inserted {inserted_count} new TV show IDs in {elapsed:.2f}s")
        
    except Exception as e:
        log_and_print(f"Error discovering TV show IDs: {e}", level='error')


# =============================================================================
# TV SHOW INFO FUNCTIONS
# =============================================================================

def fetch_tv_show_info(tv_id):
    """Fetch TV show info from TMDB API."""
    for attempt in range(MAX_RETRIES):
        try:
            tv_info = tmdb.TV(tv_id).info()
            
            # Convert complex fields to strings
            tv_info['genres'] = safe_str(tv_info.get('genres'))
            tv_info['networks'] = safe_str(tv_info.get('networks'))
            tv_info['created_by'] = safe_str(tv_info.get('created_by'))
            tv_info['production_countries'] = safe_str(tv_info.get('production_countries'))
            tv_info['production_companies'] = safe_str(tv_info.get('production_companies'))
            tv_info['spoken_languages'] = safe_str(tv_info.get('spoken_languages'))
            tv_info['origin_country'] = safe_str(tv_info.get('origin_country'))
            tv_info['episode_run_time'] = safe_str(tv_info.get('episode_run_time'))
            
            return tv_info
            
        except HTTPError as e:
            if e.response.status_code == 404:
                return None
            if e.response.status_code == 429:
                handle_rate_limit(attempt)
                continue
            log_and_print(f"HTTPError for TV show ID {tv_id}: {e}", level='error')
            return None
        except Exception as e:
            log_and_print(f"Error fetching TV show ID {tv_id}: {e}", level='error')
            return None
    
    return None


def update_tv_shows_info(con):
    """Update TV shows info data."""
    log_and_print("=" * 60)
    log_and_print("STARTING TV SHOWS INFO UPDATE")
    log_and_print("=" * 60)
    
    start_time = time.time()
    all_tv_data = []
    processed_count = 0
    skipped_ids = []
    
    processed_ids = load_checkpoint('tv_shows_info_checkpoint.pkl')
    
    # Get TV shows that need info update
    tv_ids_df = con.execute('''
        SELECT id FROM tv_shows WHERE name IS NULL OR name = ''
    ''').fetchdf()
    
    if tv_ids_df.empty:
        log_and_print("No TV shows need info updating.")
        return
    
    tv_ids_to_process = [tid for tid in tv_ids_df['id'].tolist() if tid not in processed_ids]
    total_to_process = len(tv_ids_to_process)
    
    if total_to_process == 0:
        log_and_print("All TV shows already processed (from checkpoint).")
        return
    
    log_and_print(f"Starting TV show info retrieval for {total_to_process} shows...")
    
    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        future_to_tv_id = {
            executor.submit(fetch_tv_show_info, tv_id): tv_id
            for tv_id in tv_ids_to_process
        }
        
        for future in as_completed(future_to_tv_id):
            tv_id = future_to_tv_id[future]
            try:
                tv_data = future.result()
                if tv_data:
                    all_tv_data.append(tv_data)
                else:
                    skipped_ids.append(tv_id)
                
                processed_ids.add(tv_id)
                processed_count += 1
                
                if processed_count % API_BATCH_SIZE == 0:
                    percent = (processed_count / total_to_process) * 100
                    log_and_print(f"TV Show Info Progress: {processed_count}/{total_to_process} ({percent:.2f}%)")
                    save_checkpoint(processed_ids, 'tv_shows_info_checkpoint.pkl')
                
            except Exception as e:
                log_and_print(f"Error processing TV show ID {tv_id}: {e}", level='error')
                skipped_ids.append(tv_id)
    
    elapsed = time.time() - start_time
    log_and_print(f"TV show info API retrieval complete in {elapsed:.2f}s")
    log_and_print(f"Successfully fetched: {len(all_tv_data)}, Skipped: {len(skipped_ids)}")
    
    if not all_tv_data:
        log_and_print("No TV show data to update.")
        log_skipped_ids(skipped_ids, 'tv_shows_info_skipped_ids.log')
        return
    
    tv_df = pd.DataFrame(all_tv_data)
    log_null_columns(tv_df, log_file='tv_shows_info_null_columns.log')
    
    log_and_print("Updating TV shows in database...")
    
    num_batches = math.ceil(len(tv_df) / DB_INSERT_BATCH_SIZE)
    for i in range(num_batches):
        start_idx = i * DB_INSERT_BATCH_SIZE
        end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(tv_df))
        batch_df = tv_df.iloc[start_idx:end_idx]
        
        try:
            con.register('batch_view', batch_df)
            con.execute('''
                UPDATE tv_shows
                SET
                    name = batch_view.name,
                    overview = batch_view.overview,
                    poster_path = batch_view.poster_path,
                    backdrop_path = batch_view.backdrop_path,
                    popularity = batch_view.popularity,
                    vote_average = batch_view.vote_average,
                    vote_count = batch_view.vote_count,
                    first_air_date = batch_view.first_air_date,
                    last_air_date = batch_view.last_air_date,
                    episode_run_time = batch_view.episode_run_time,
                    homepage = batch_view.homepage,
                    in_production = batch_view.in_production,
                    number_of_episodes = batch_view.number_of_episodes,
                    number_of_seasons = batch_view.number_of_seasons,
                    origin_country = batch_view.origin_country,
                    original_language = batch_view.original_language,
                    original_name = batch_view.original_name,
                    production_countries = batch_view.production_countries,
                    genres = batch_view.genres,
                    networks = batch_view.networks,
                    created_by = batch_view.created_by,
                    status = batch_view.status,
                    type = batch_view.type,
                    tagline = batch_view.tagline,
                    adult = batch_view.adult
                FROM batch_view
                WHERE tv_shows.id = batch_view.id
            ''')
            log_and_print(f"TV Show Info Batch {i+1}/{num_batches} updated ({len(batch_df)} rows)")
        except Exception as e:
            log_and_print(f"Error updating TV show batch: {e}", level='error')
    
    save_checkpoint(processed_ids, 'tv_shows_info_checkpoint.pkl')
    log_skipped_ids(skipped_ids, 'tv_shows_info_skipped_ids.log')
    
    total_elapsed = time.time() - start_time
    log_and_print(f'TV shows info update complete in {total_elapsed:.2f}s')


# =============================================================================
# TV SHOW CAST/CREW FUNCTIONS
# =============================================================================

def fetch_tv_show_credits(tv_id):
    """Fetches credits for the most recent season of a TV show."""
    for attempt in range(MAX_RETRIES):
        try:
            tv = tmdb.TV(tv_id)
            
            try:
                tv_info = tv.info()
            except HTTPError as e:
                if e.response.status_code == 404:
                    return []
                if e.response.status_code == 429:
                    handle_rate_limit(attempt)
                    continue
                raise
            
            seasons = tv_info.get('seasons', [])
            if not seasons:
                return []
            
            seasons_sorted = sorted(
                [s for s in seasons if s.get('season_number') is not None],
                key=lambda s: (s.get('air_date') or '', s['season_number']),
                reverse=True
            )
            
            if not seasons_sorted:
                return []
            
            recent_season = seasons_sorted[0]
            season_number = recent_season['season_number']
            season = tmdb.TV_Seasons(tv_id, season_number)
            
            try:
                credits = season.credits()
            except HTTPError as e:
                if e.response.status_code == 404:
                    return []
                if e.response.status_code == 429:
                    handle_rate_limit(attempt)
                    continue
                raise
            
            cast_list = credits.get('cast', [])
            processed_cast_data = []
            
            for cast_member in cast_list:
                roles = cast_member.get('roles')
                
                character = cast_member.get('character')
                if not character and roles and isinstance(roles, list) and len(roles) > 0:
                    character = roles[0].get('character')
                
                cast_order = cast_member.get('order')
                if cast_order is None:
                    cast_order = cast_member.get('cast_order')
                
                credit_id = cast_member.get('credit_id')
                if not credit_id and roles and isinstance(roles, list) and len(roles) > 0:
                    credit_id = roles[0].get('credit_id')
                
                cast_id = cast_member.get('cast_id')
                if cast_id is None:
                    cast_id = cast_member.get('id')
                
                also_known_as = cast_member.get('also_known_as')
                if also_known_as and isinstance(also_known_as, list):
                    also_known_as = str(also_known_as)
                
                total_episode_count = cast_member.get('total_episode_count')
                if total_episode_count is None and roles and isinstance(roles, list):
                    total_episode_count = sum(r.get('episode_count', 0) for r in roles)
                
                processed_cast_data.append({
                    'tv_id': tv_id,
                    'person_id': cast_member.get('id'),
                    'name': cast_member.get('name'),
                    'credit_id': credit_id,
                    'character': character,
                    'cast_order': cast_order,
                    'gender': cast_member.get('gender'),
                    'profile_path': cast_member.get('profile_path'),
                    'known_for_department': cast_member.get('known_for_department'),
                    'popularity': cast_member.get('popularity'),
                    'original_name': cast_member.get('original_name'),
                    'roles': str(roles) if roles else None,
                    'total_episode_count': total_episode_count,
                    'cast_id': cast_id,
                    'also_known_as': also_known_as
                })
            return processed_cast_data
            
        except HTTPError as e:
            if e.response.status_code == 429:
                handle_rate_limit(attempt)
                continue
            log_and_print(f"HTTPError for tv_id {tv_id}: {e}", level='error')
            return []
        except Exception as e:
            log_and_print(f"Error fetching credits for tv_id {tv_id}: {e}", level='error')
            return []
    
    return []


def check_and_remove_tv_duplicates(con):
    """Check for and remove duplicate TV cast/crew rows."""
    log_and_print("Checking for TV cast/crew duplicates...")
    
    try:
        dup_count_result = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT tv_id, person_id, credit_id
                FROM tv_show_cast_crew
                GROUP BY tv_id, person_id, credit_id
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
    except Exception:
        log_and_print("tv_show_cast_crew table doesn't exist or is empty")
        return
    
    if dup_count_result == 0:
        log_and_print("No TV cast/crew duplicates found.")
        return
    
    log_and_print(f"Found {dup_count_result} TV duplicate groups. Removing...")
    
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tv_dedup_keep AS
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY tv_id, person_id, credit_id
                    ORDER BY inserted_at DESC
                ) AS rn
            FROM tv_show_cast_crew
        )
        WHERE rn = 1
    """)
    
    before_count = con.execute("SELECT COUNT(*) FROM tv_show_cast_crew").fetchone()[0]
    
    con.execute("DELETE FROM tv_show_cast_crew")
    con.execute("""
        INSERT INTO tv_show_cast_crew
        SELECT tv_id, person_id, name, credit_id, character, cast_order, gender,
               profile_path, known_for_department, popularity, original_name, roles,
               total_episode_count, cast_id, also_known_as, inserted_at, updated_at
        FROM tv_dedup_keep
    """)
    
    after_count = con.execute("SELECT COUNT(*) FROM tv_show_cast_crew").fetchone()[0]
    deleted = before_count - after_count
    
    con.execute("DROP TABLE IF EXISTS tv_dedup_keep")
    log_and_print(f"TV deduplication complete. Removed {deleted} duplicate rows.")


def update_tv_show_cast_crew(con):
    """Update TV show cast/crew data."""
    log_and_print("=" * 60)
    log_and_print("STARTING TV SHOW CAST/CREW UPDATE")
    log_and_print("=" * 60)
    
    start_time = time.time()
    all_cast_data_flat = []
    processed_tv_count = 0
    skipped_ids = []

    processed_ids = load_checkpoint('tv_cast_crew_checkpoint.pkl')

    log_and_print('Fetching TV show IDs from MotherDuck...')
    
    try:
        tv_ids_df = con.execute(
            '''SELECT id FROM tv_shows WHERE id NOT IN (SELECT DISTINCT tv_id FROM tv_show_cast_crew)'''
        ).fetchdf()
    except Exception:
        tv_ids_df = con.execute('SELECT id FROM tv_shows').fetchdf()
    
    tv_ids_to_process = [tid for tid in tv_ids_df['id'].tolist() if tid not in processed_ids]
    total_tv_to_process = len(tv_ids_to_process)
    log_and_print(f"Found {total_tv_to_process} TV show IDs to process.")

    if total_tv_to_process == 0:
        log_and_print("No new TV shows to process.")
        check_and_remove_tv_duplicates(con)
        return

    log_and_print(f'Starting parallel API retrieval with {MAX_API_WORKERS} workers...')

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        future_to_tv_id = {
            executor.submit(fetch_tv_show_credits, tv_id): tv_id
            for tv_id in tv_ids_to_process
        }
        for future in as_completed(future_to_tv_id):
            tv_id = future_to_tv_id[future]
            try:
                cast_data = future.result()
                if cast_data:
                    all_cast_data_flat.extend(cast_data)
                else:
                    skipped_ids.append(tv_id)
                processed_ids.add(tv_id)
                processed_tv_count += 1
                if processed_tv_count % API_BATCH_SIZE == 0:
                    percent = (processed_tv_count / total_tv_to_process) * 100
                    log_and_print(f"TV Cast/Crew Progress: {processed_tv_count}/{total_tv_to_process} ({percent:.2f}%)")
                    save_checkpoint(processed_ids, 'tv_cast_crew_checkpoint.pkl')
            except Exception as e:
                log_and_print(f"Error processing tv_id {tv_id}: {e}", level='error')
                skipped_ids.append(tv_id)

    elapsed = time.time() - start_time
    log_and_print(f'Finished TV API retrieval in {elapsed:.2f}s')
    log_and_print(f'Total TV cast/crew members fetched: {len(all_cast_data_flat)}')

    if not all_cast_data_flat:
        log_and_print("No TV cast/crew data to insert.")
        check_and_remove_tv_duplicates(con)
        log_skipped_ids(skipped_ids, 'tv_cast_crew_skipped_ids.log')
        return

    cast_df = pd.DataFrame(all_cast_data_flat)
    log_null_columns(cast_df, log_file='tv_cast_crew_null_columns.log')

    log_and_print('Inserting TV data into MotherDuck...')
    tv_columns = 'tv_id, person_id, name, credit_id, character, cast_order, gender, profile_path, known_for_department, popularity, original_name, roles, total_episode_count, cast_id, also_known_as'
    
    num_batches = math.ceil(len(cast_df) / DB_INSERT_BATCH_SIZE)
    for i in range(num_batches):
        start_idx = i * DB_INSERT_BATCH_SIZE
        end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(cast_df))
        batch_df = cast_df.iloc[start_idx:end_idx]
        try:
            con.register('batch_df_view', batch_df)
            con.execute(f"INSERT INTO tv_show_cast_crew ({tv_columns}) SELECT {tv_columns} FROM batch_df_view;")
            log_and_print(f"TV Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows)")
        except Exception as e:
            log_and_print(f"Error inserting TV batch: {e}", level='error')

    save_checkpoint(processed_ids, 'tv_cast_crew_checkpoint.pkl')
    check_and_remove_tv_duplicates(con)
    log_skipped_ids(skipped_ids, 'tv_cast_crew_skipped_ids.log')
    
    total_elapsed = time.time() - start_time
    log_and_print(f'TV show cast/crew update complete in {total_elapsed:.2f}s')


# =============================================================================
# MOVIE CAST FUNCTIONS
# =============================================================================

def fetch_movie_cast(movie_id):
    """Fetches cast for a single movie ID."""
    for attempt in range(MAX_RETRIES):
        try:
            try:
                credits_dict = tmdb.Movies(movie_id).credits()
            except HTTPError as e:
                if e.response.status_code == 404:
                    return []
                if e.response.status_code == 429:
                    handle_rate_limit(attempt)
                    continue
                raise
            
            cast_list = credits_dict.get("cast", [])
            processed_cast_data = []
            
            for cast_member_dict in cast_list:
                processed_cast_data.append({
                    'movie_id': movie_id,
                    'person_id': cast_member_dict.get('id'),
                    'name': cast_member_dict.get('name'),
                    'credit_id': cast_member_dict.get('credit_id'),
                    'character': cast_member_dict.get('character'),
                    'cast_order': cast_member_dict.get('order'),
                    'gender': cast_member_dict.get('gender'),
                    'profile_path': cast_member_dict.get('profile_path'),
                    'known_for_department': cast_member_dict.get('known_for_department'),
                    'popularity': cast_member_dict.get('popularity'),
                    'original_name': cast_member_dict.get('original_name'),
                    'cast_id': cast_member_dict.get('cast_id'),
                })
            return processed_cast_data
            
        except HTTPError as e:
            if e.response.status_code == 429:
                handle_rate_limit(attempt)
                continue
            log_and_print(f"HTTPError for movie cast {movie_id}: {e}", level='error')
            return []
        except Exception as e:
            log_and_print(f"Error fetching cast for movie ID {movie_id}: {e}", level='error')
            return []
    
    return []


def check_and_remove_movie_cast_duplicates(con):
    """Check for and remove duplicate movie cast rows."""
    log_and_print("Checking for movie cast duplicates...")
    
    try:
        dup_count = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT movie_id, person_id, credit_id
                FROM movie_cast
                GROUP BY movie_id, person_id, credit_id
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
    except Exception:
        log_and_print("movie_cast table doesn't exist or is empty")
        return
    
    if dup_count == 0:
        log_and_print("No movie cast duplicates found.")
        return
    
    log_and_print(f"Found {dup_count} movie cast duplicate groups. Removing...")
    
    con.execute("""
        CREATE OR REPLACE TEMP TABLE movie_cast_dedup AS
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY movie_id, person_id, credit_id
                    ORDER BY inserted_at DESC
                ) AS rn
            FROM movie_cast
        )
        WHERE rn = 1
    """)
    
    before_count = con.execute("SELECT COUNT(*) FROM movie_cast").fetchone()[0]
    
    con.execute("DELETE FROM movie_cast")
    con.execute("""
        INSERT INTO movie_cast
        SELECT movie_id, person_id, name, credit_id, character, cast_order, gender,
               profile_path, known_for_department, popularity, original_name, cast_id,
               inserted_at, updated_at
        FROM movie_cast_dedup
    """)
    
    after_count = con.execute("SELECT COUNT(*) FROM movie_cast").fetchone()[0]
    deleted = before_count - after_count
    
    con.execute("DROP TABLE IF EXISTS movie_cast_dedup")
    log_and_print(f"Movie cast deduplication complete. Removed {deleted} duplicate rows.")


def update_movie_cast(con):
    """Update movie cast data."""
    log_and_print("=" * 60)
    log_and_print("STARTING MOVIE CAST UPDATE")
    log_and_print("=" * 60)
    
    start_time = time.time()
    all_cast_data_flat = []
    processed_count = 0
    skipped_ids = []

    processed_ids = load_checkpoint('movie_cast_checkpoint.pkl')

    log_and_print('Fetching movie IDs from MotherDuck...')
    
    try:
        movies_ids_df = con.execute(
            '''SELECT id FROM movies WHERE id NOT IN (SELECT DISTINCT movie_id FROM movie_cast)'''
        ).fetchdf()
    except Exception:
        movies_ids_df = con.execute('SELECT id FROM movies').fetchdf()
    
    movie_ids_to_process = [mid for mid in movies_ids_df['id'].tolist() if mid not in processed_ids]
    total_to_process = len(movie_ids_to_process)
    log_and_print(f"Found {total_to_process} movie IDs to process.")

    if total_to_process == 0:
        log_and_print("No new movies to process for cast.")
        check_and_remove_movie_cast_duplicates(con)
        return

    log_and_print(f'Starting parallel API retrieval with {MAX_API_WORKERS} workers...')

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        future_to_movie_id = {
            executor.submit(fetch_movie_cast, movie_id): movie_id
            for movie_id in movie_ids_to_process
        }

        for future in as_completed(future_to_movie_id):
            movie_id = future_to_movie_id[future]
            try:
                cast_data = future.result()
                if cast_data:
                    all_cast_data_flat.extend(cast_data)
                else:
                    skipped_ids.append(movie_id)
                
                processed_ids.add(movie_id)
                processed_count += 1

                if processed_count % API_BATCH_SIZE == 0:
                    percent = (processed_count / total_to_process) * 100
                    log_and_print(f"Movie Cast Progress: {processed_count}/{total_to_process} ({percent:.2f}%)")
                    save_checkpoint(processed_ids, 'movie_cast_checkpoint.pkl')
                    
            except Exception as e:
                log_and_print(f"Error processing movie ID {movie_id}: {e}", level='error')
                skipped_ids.append(movie_id)

    elapsed = time.time() - start_time
    log_and_print(f'Finished movie cast API retrieval in {elapsed:.2f}s')
    log_and_print(f'Total cast members fetched: {len(all_cast_data_flat)}')

    if not all_cast_data_flat:
        log_and_print("No movie cast data to insert.")
        check_and_remove_movie_cast_duplicates(con)
        log_skipped_ids(skipped_ids, 'movie_cast_skipped_ids.log')
        return

    cast_df = pd.DataFrame(all_cast_data_flat)
    log_null_columns(cast_df, log_file='movie_cast_null_columns.log')

    log_and_print('Inserting movie cast data into MotherDuck...')
    cast_columns = 'movie_id, person_id, name, credit_id, character, cast_order, gender, profile_path, known_for_department, popularity, original_name, cast_id'

    num_batches = math.ceil(len(cast_df) / DB_INSERT_BATCH_SIZE)
    for i in range(num_batches):
        start_idx = i * DB_INSERT_BATCH_SIZE
        end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(cast_df))
        batch_df = cast_df.iloc[start_idx:end_idx]
        try:
            con.register('batch_df_view', batch_df)
            con.execute(f"INSERT INTO movie_cast ({cast_columns}) SELECT {cast_columns} FROM batch_df_view;")
            log_and_print(f"Movie Cast Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows)")
        except Exception as e:
            log_and_print(f"Error inserting movie cast batch: {e}", level='error')

    save_checkpoint(processed_ids, 'movie_cast_checkpoint.pkl')
    check_and_remove_movie_cast_duplicates(con)
    log_skipped_ids(skipped_ids, 'movie_cast_skipped_ids.log')

    total_elapsed = time.time() - start_time
    log_and_print(f'Movie cast update complete in {total_elapsed:.2f}s')


# =============================================================================
# MOVIE CREW FUNCTIONS
# =============================================================================

def fetch_movie_crew(movie_id):
    """Fetches crew for a single movie ID."""
    for attempt in range(MAX_RETRIES):
        try:
            try:
                credits_dict = tmdb.Movies(movie_id).credits()
            except HTTPError as e:
                if e.response.status_code == 404:
                    return []
                if e.response.status_code == 429:
                    handle_rate_limit(attempt)
                    continue
                raise
            
            crew_list = credits_dict.get("crew", [])
            processed_crew_data = []
            
            # Limit crew to 50 per movie
            for crew_member_dict in crew_list[:50]:
                processed_crew_data.append({
                    'movie_id': movie_id,
                    'person_id': crew_member_dict.get('id'),
                    'name': crew_member_dict.get('name'),
                    'credit_id': crew_member_dict.get('credit_id'),
                    'gender': crew_member_dict.get('gender'),
                    'profile_path': crew_member_dict.get('profile_path'),
                    'known_for_department': crew_member_dict.get('known_for_department'),
                    'popularity': crew_member_dict.get('popularity'),
                    'original_name': crew_member_dict.get('original_name'),
                    'adult': crew_member_dict.get('adult'),
                    'department': crew_member_dict.get('department'),
                    'job': crew_member_dict.get('job')
                })
            return processed_crew_data
            
        except HTTPError as e:
            if e.response.status_code == 429:
                handle_rate_limit(attempt)
                continue
            log_and_print(f"HTTPError for movie crew {movie_id}: {e}", level='error')
            return []
        except Exception as e:
            log_and_print(f"Error fetching crew for movie ID {movie_id}: {e}", level='error')
            return []
    
    return []


def check_and_remove_movie_crew_duplicates(con):
    """Check for and remove duplicate movie crew rows."""
    log_and_print("Checking for movie crew duplicates...")
    
    try:
        dup_count = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT movie_id, person_id, credit_id
                FROM movie_crew
                GROUP BY movie_id, person_id, credit_id
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
    except Exception:
        log_and_print("movie_crew table doesn't exist or is empty")
        return
    
    if dup_count == 0:
        log_and_print("No movie crew duplicates found.")
        return
    
    log_and_print(f"Found {dup_count} movie crew duplicate groups. Removing...")
    
    con.execute("""
        CREATE OR REPLACE TEMP TABLE movie_crew_dedup AS
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY movie_id, person_id, credit_id
                    ORDER BY inserted_at DESC
                ) AS rn
            FROM movie_crew
        )
        WHERE rn = 1
    """)
    
    before_count = con.execute("SELECT COUNT(*) FROM movie_crew").fetchone()[0]
    
    con.execute("DELETE FROM movie_crew")
    con.execute("""
        INSERT INTO movie_crew
        SELECT movie_id, person_id, name, credit_id, gender, profile_path,
               known_for_department, popularity, original_name, adult,
               department, job, inserted_at, updated_at
        FROM movie_crew_dedup
    """)
    
    after_count = con.execute("SELECT COUNT(*) FROM movie_crew").fetchone()[0]
    deleted = before_count - after_count
    
    con.execute("DROP TABLE IF EXISTS movie_crew_dedup")
    log_and_print(f"Movie crew deduplication complete. Removed {deleted} duplicate rows.")


def update_movie_crew(con):
    """Update movie crew data."""
    log_and_print("=" * 60)
    log_and_print("STARTING MOVIE CREW UPDATE")
    log_and_print("=" * 60)
    
    start_time = time.time()
    all_crew_data_flat = []
    processed_count = 0
    skipped_ids = []

    processed_ids = load_checkpoint('movie_crew_checkpoint.pkl')

    log_and_print('Fetching movie IDs from MotherDuck...')
    
    try:
        movies_ids_df = con.execute(
            '''SELECT id FROM movies WHERE id NOT IN (SELECT DISTINCT movie_id FROM movie_crew)'''
        ).fetchdf()
    except Exception:
        movies_ids_df = con.execute('SELECT id FROM movies').fetchdf()
    
    movie_ids_to_process = [mid for mid in movies_ids_df['id'].tolist() if mid not in processed_ids]
    total_to_process = len(movie_ids_to_process)
    log_and_print(f"Found {total_to_process} movie IDs to process.")

    if total_to_process == 0:
        log_and_print("No new movies to process for crew.")
        check_and_remove_movie_crew_duplicates(con)
        return

    log_and_print(f'Starting parallel API retrieval with {MAX_API_WORKERS} workers...')

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        future_to_movie_id = {
            executor.submit(fetch_movie_crew, movie_id): movie_id
            for movie_id in movie_ids_to_process
        }

        for future in as_completed(future_to_movie_id):
            movie_id = future_to_movie_id[future]
            try:
                crew_data = future.result()
                if crew_data:
                    all_crew_data_flat.extend(crew_data)
                else:
                    skipped_ids.append(movie_id)
                
                processed_ids.add(movie_id)
                processed_count += 1

                if processed_count % API_BATCH_SIZE == 0:
                    percent = (processed_count / total_to_process) * 100
                    log_and_print(f"Movie Crew Progress: {processed_count}/{total_to_process} ({percent:.2f}%)")
                    save_checkpoint(processed_ids, 'movie_crew_checkpoint.pkl')
                    
            except Exception as e:
                log_and_print(f"Error processing movie ID {movie_id}: {e}", level='error')
                skipped_ids.append(movie_id)

    elapsed = time.time() - start_time
    log_and_print(f'Finished movie crew API retrieval in {elapsed:.2f}s')
    log_and_print(f'Total crew members fetched: {len(all_crew_data_flat)}')

    if not all_crew_data_flat:
        log_and_print("No movie crew data to insert.")
        check_and_remove_movie_crew_duplicates(con)
        log_skipped_ids(skipped_ids, 'movie_crew_skipped_ids.log')
        return

    crew_df = pd.DataFrame(all_crew_data_flat)
    log_null_columns(crew_df, log_file='movie_crew_null_columns.log')

    log_and_print('Inserting movie crew data into MotherDuck...')
    crew_columns = 'movie_id, person_id, name, credit_id, gender, profile_path, known_for_department, popularity, original_name, adult, department, job'

    num_batches = math.ceil(len(crew_df) / DB_INSERT_BATCH_SIZE)
    for i in range(num_batches):
        start_idx = i * DB_INSERT_BATCH_SIZE
        end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(crew_df))
        batch_df = crew_df.iloc[start_idx:end_idx]
        try:
            con.register('batch_df_view', batch_df)
            con.execute(f"INSERT INTO movie_crew ({crew_columns}) SELECT {crew_columns} FROM batch_df_view;")
            log_and_print(f"Movie Crew Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows)")
        except Exception as e:
            log_and_print(f"Error inserting movie crew batch: {e}", level='error')

    save_checkpoint(processed_ids, 'movie_crew_checkpoint.pkl')
    check_and_remove_movie_crew_duplicates(con)
    log_skipped_ids(skipped_ids, 'movie_crew_skipped_ids.log')

    total_elapsed = time.time() - start_time
    log_and_print(f'Movie crew update complete in {total_elapsed:.2f}s')


# =============================================================================
# MOVIES INFO FUNCTIONS
# =============================================================================

def fetch_movie_info(movie_id):
    """Fetch movie info from TMDB API."""
    for attempt in range(MAX_RETRIES):
        try:
            movie_info = tmdb.Movies(movie_id).info()
            
            movie_info['production_countries'] = safe_str(movie_info.get('production_countries'))
            movie_info['genres'] = safe_str(movie_info.get('genres'))
            movie_info['production_companies'] = safe_str(movie_info.get('production_companies'))
            movie_info['spoken_languages'] = safe_str(movie_info.get('spoken_languages'))
            movie_info['belongs_to_collection'] = safe_str(movie_info.get('belongs_to_collection'))
            
            if 'origin_country' in movie_info:
                origin = movie_info.get('origin_country')
                movie_info['origin_country'] = origin if isinstance(origin, list) else None
            
            return movie_info
            
        except HTTPError as e:
            if e.response.status_code == 404:
                return None
            if e.response.status_code == 429:
                handle_rate_limit(attempt)
                continue
            log_and_print(f"HTTPError for movie ID {movie_id}: {e}", level='error')
            return None
        except Exception as e:
            log_and_print(f"Error fetching movie ID {movie_id}: {e}", level='error')
            return None
    
    return None


def update_movies_info(con):
    """Update movies info data."""
    log_and_print("=" * 60)
    log_and_print("STARTING MOVIES INFO UPDATE")
    log_and_print("=" * 60)
    
    start_time = time.time()
    all_movie_data = []
    processed_count = 0
    skipped_ids = []
    
    processed_ids = load_checkpoint('movies_info_checkpoint.pkl')
    
    movies_ids_df = con.execute('''
        SELECT id FROM movies WHERE title IS NULL OR title = ''
    ''').fetchdf()
    
    if movies_ids_df.empty:
        log_and_print("No movies need info updating.")
        return
    
    movie_ids_to_process = [mid for mid in movies_ids_df['id'].tolist() if mid not in processed_ids]
    total_to_process = len(movie_ids_to_process)
    
    if total_to_process == 0:
        log_and_print("All movies already processed (from checkpoint).")
        return
    
    log_and_print(f"Starting movie info retrieval for {total_to_process} movies...")
    
    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        future_to_movie_id = {
            executor.submit(fetch_movie_info, movie_id): movie_id
            for movie_id in movie_ids_to_process
        }
        
        for future in as_completed(future_to_movie_id):
            movie_id = future_to_movie_id[future]
            try:
                movie_data = future.result()
                if movie_data:
                    all_movie_data.append(movie_data)
                else:
                    skipped_ids.append(movie_id)
                
                processed_ids.add(movie_id)
                processed_count += 1
                
                if processed_count % API_BATCH_SIZE == 0:
                    percent = (processed_count / total_to_process) * 100
                    log_and_print(f"Movie Info Progress: {processed_count}/{total_to_process} ({percent:.2f}%)")
                    save_checkpoint(processed_ids, 'movies_info_checkpoint.pkl')
                
            except Exception as e:
                log_and_print(f"Error processing movie ID {movie_id}: {e}", level='error')
                skipped_ids.append(movie_id)
    
    elapsed = time.time() - start_time
    log_and_print(f"Movie info API retrieval complete in {elapsed:.2f}s")
    log_and_print(f"Successfully fetched: {len(all_movie_data)}, Skipped: {len(skipped_ids)}")
    
    if not all_movie_data:
        log_and_print("No movie data to update.")
        log_skipped_ids(skipped_ids, 'movies_info_skipped_ids.log')
        return
    
    movies_df = pd.DataFrame(all_movie_data)
    log_null_columns(movies_df, log_file='movies_info_null_columns.log')
    
    log_and_print("Updating movies in database...")
    
    num_batches = math.ceil(len(movies_df) / DB_INSERT_BATCH_SIZE)
    for i in range(num_batches):
        start_idx = i * DB_INSERT_BATCH_SIZE
        end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(movies_df))
        batch_df = movies_df.iloc[start_idx:end_idx]
        
        try:
            con.register('batch_view', batch_df)
            con.execute('''
                UPDATE movies
                SET
                    adult = batch_view.adult,
                    backdrop_path = batch_view.backdrop_path,
                    belongs_to_collection = batch_view.belongs_to_collection,
                    budget = batch_view.budget,
                    genres = batch_view.genres,
                    homepage = batch_view.homepage,
                    imdb_id = batch_view.imdb_id,
                    origin_country = batch_view.origin_country,
                    original_language = batch_view.original_language,
                    original_title = batch_view.original_title,
                    overview = batch_view.overview,
                    popularity = batch_view.popularity,
                    poster_path = batch_view.poster_path,
                    production_companies = batch_view.production_companies,
                    production_countries = batch_view.production_countries,
                    release_date = batch_view.release_date,
                    revenue = batch_view.revenue,
                    runtime = batch_view.runtime,
                    spoken_languages = batch_view.spoken_languages,
                    status = batch_view.status,
                    tagline = batch_view.tagline,
                    title = batch_view.title,
                    video = batch_view.video,
                    vote_average = batch_view.vote_average,
                    vote_count = batch_view.vote_count
                FROM batch_view
                WHERE movies.id = batch_view.id
            ''')
            log_and_print(f"Movie Info Batch {i+1}/{num_batches} updated ({len(batch_df)} rows)")
        except Exception as e:
            log_and_print(f"Error updating movie batch: {e}", level='error')
    
    save_checkpoint(processed_ids, 'movies_info_checkpoint.pkl')
    log_skipped_ids(skipped_ids, 'movies_info_skipped_ids.log')
    
    total_elapsed = time.time() - start_time
    log_and_print(f'Movies info update complete in {total_elapsed:.2f}s')


# =============================================================================
# MAIN UPDATE JOB
# =============================================================================

def run_update_job():
    """Main entry point for the update job."""
    log_and_print("=" * 60)
    log_and_print("STARTING FULL UPDATE JOB")
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
        # Ensure all tables exist
        ensure_movies_table(con)
        ensure_tv_shows_table(con)
        ensure_cast_crew_tables(con)
        
        # 1. Discover new IDs from TMDB exports
        discover_new_movie_ids(con)
        discover_new_tv_show_ids(con)
        
        # 2. Update info for new IDs
        update_movies_info(con)
        update_tv_shows_info(con)
        
        # 3. Update cast/crew for new IDs
        update_movie_cast(con)
        update_movie_crew(con)
        update_tv_show_cast_crew(con)
        
    except Exception as e:
        log_and_print(f"Critical error in update job: {e}", level='error')
        import traceback
        log_and_print(traceback.format_exc(), level='error')
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    
    total_elapsed = time.time() - start_time
    hours, remainder = divmod(total_elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    log_and_print("=" * 60)
    log_and_print(f"FULL UPDATE JOB COMPLETE in {int(hours)}h {int(minutes)}m {seconds:.2f}s")
    log_and_print("=" * 60)


if __name__ == "__main__":
    run_update_job()
