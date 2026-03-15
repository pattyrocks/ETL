import tmdbsimple as tmdb
import os
import time
import duckdb
import pandas as pd
import logging
import math
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from requests.exceptions import HTTPError

# --- Configuration ---
API_KEY = os.getenv('TMDBAPIKEY')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
DATABASE_PATH = f'md:TMDB?motherduck_token={MOTHERDUCK_TOKEN}'

tmdb.API_KEY = API_KEY

MAX_API_WORKERS = 15
DB_INSERT_BATCH_SIZE = 5000
API_BATCH_SIZE = 500
MAX_RETRIES = 3

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


def log_and_print(message, level='info'):
    """Log to file and print message to console."""
    getattr(logger, level)(message)


def save_checkpoint(processed_ids, filename):
    """Save processed IDs to a checkpoint file."""
    import pickle
    with open(filename, 'wb') as f:
        pickle.dump(processed_ids, f)


def load_checkpoint(filename):
    """Load processed IDs from a checkpoint file."""
    import pickle
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return set()


def log_null_columns(df, log_file):
    """Log columns with null values."""
    null_counts = df.isnull().sum()
    with open(log_file, 'w') as f:
        for col, count in null_counts.items():
            if count > 0:
                f.write(f"{col}: {count} nulls\n")
    log_and_print(f"Null column log written to {log_file}")


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
            
            for cast_member in cast_list:  # No limit for cast
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
            
        except Exception as e:
            log_and_print(f"Error fetching credits for tv_id {tv_id}: {e}", level='error')
            return []


def check_and_remove_tv_duplicates(con):
    """Check for and remove duplicate TV cast/crew rows."""
    log_and_print("Checking for TV cast/crew duplicates...")
    
    dup_count_result = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT tv_id, person_id, credit_id
            FROM tv_show_cast_crew
            GROUP BY tv_id, person_id, credit_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    
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
                    ORDER BY inserted_at
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

    checkpoint = load_checkpoint('tv_checkpoint.pkl')
    processed_ids = checkpoint if checkpoint else set()

    log_and_print('Fetching TV show IDs from MotherDuck...')
    tv_ids_df = con.execute(
        '''SELECT id FROM tv_shows WHERE id NOT IN (SELECT DISTINCT tv_id FROM tv_show_cast_crew)'''
    ).fetchdf()
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
                    log_and_print(f"TV Progress: {processed_tv_count}/{total_tv_to_process} ({percent:.2f}%)")
                    save_checkpoint(processed_ids, 'tv_checkpoint.pkl')
            except Exception as e:
                log_and_print(f"Error processing tv_id {tv_id}: {e}", level='error')
                skipped_ids.append(tv_id)

    elapsed = time.time() - start_time
    log_and_print(f'Finished TV API retrieval in {elapsed:.2f}s')
    log_and_print(f'Total TV cast/crew members fetched: {len(all_cast_data_flat)}')

    if not all_cast_data_flat:
        log_and_print("No TV cast/crew data to insert.")
        check_and_remove_tv_duplicates(con)
        return

    cast_df = pd.DataFrame(all_cast_data_flat)
    cast_df = cast_df.groupby('tv_id', group_keys=False).head(50)

    log_null_columns(cast_df, log_file='tv_null_columns.log')

    log_and_print('Inserting TV data into MotherDuck...')
    tv_columns = 'tv_id, person_id, name, credit_id, character, cast_order, gender, profile_path, known_for_department, popularity, original_name, roles, total_episode_count, cast_id, also_known_as'
    
    num_batches = math.ceil(len(cast_df) / DB_INSERT_BATCH_SIZE)
    for i in range(num_batches):
        start_idx = i * DB_INSERT_BATCH_SIZE
        end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(cast_df))
        batch_df = cast_df.iloc[start_idx:end_idx]
        con.register('batch_df_view', batch_df)
        con.execute(f"INSERT INTO tv_show_cast_crew ({tv_columns}) SELECT {tv_columns} FROM batch_df_view;")
        log_and_print(f"TV Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows)")

    save_checkpoint(processed_ids, 'tv_checkpoint.pkl')
    check_and_remove_tv_duplicates(con)
    
    if skipped_ids:
        log_and_print(f"Skipped {len(skipped_ids)} TV IDs", level='warning')
        with open('tv_skipped_ids.log', 'w') as f:
            for sid in skipped_ids:
                f.write(f"{sid}\n")
    
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
            
            for cast_member_dict in cast_list:  # No limit for cast
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
            
        except Exception as e:
            log_and_print(f"Error fetching cast for movie ID {movie_id}: {e}", level='error')
            return []


def check_and_remove_movie_cast_duplicates(con):
    """Check for and remove duplicate movie cast rows."""
    log_and_print("Checking for movie cast duplicates...")
    
    dup_count = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT movie_id, person_id, credit_id
            FROM movie_cast
            GROUP BY movie_id, person_id, credit_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    
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
                    ORDER BY inserted_at
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

    checkpoint = load_checkpoint('movie_cast_checkpoint.pkl')
    processed_ids = checkpoint if checkpoint else set()

    log_and_print('Fetching movie IDs from MotherDuck...')
    movies_ids_df = con.execute(
        '''SELECT id FROM movies WHERE id NOT IN (SELECT DISTINCT movie_id FROM movie_cast)'''
    ).fetchdf()
    
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
        con.register('batch_df_view', batch_df)
        con.execute(f"INSERT INTO movie_cast ({cast_columns}) SELECT {cast_columns} FROM batch_df_view;")
        log_and_print(f"Movie Cast Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows)")

    save_checkpoint(processed_ids, 'movie_cast_checkpoint.pkl')
    check_and_remove_movie_cast_duplicates(con)

    if skipped_ids:
        log_and_print(f"Skipped {len(skipped_ids)} movie IDs for cast", level='warning')
        with open('movie_cast_skipped_ids.log', 'w') as f:
            for sid in skipped_ids:
                f.write(f"{sid}\n")

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
        except Exception as e:
            log_and_print(f"Error fetching crew for movie ID {movie_id}: {e}", level='error')
            return []


def check_and_remove_movie_crew_duplicates(con):
    """Check for and remove duplicate movie crew rows."""
    log_and_print("Checking for movie crew duplicates...")
    
    dup_count = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT movie_id, person_id, credit_id
            FROM movie_crew
            GROUP BY movie_id, person_id, credit_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    
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
                    ORDER BY inserted_at
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

    checkpoint = load_checkpoint('movie_crew_checkpoint.pkl')
    processed_ids = checkpoint if checkpoint else set()

    log_and_print('Fetching movie IDs from MotherDuck...')
    movies_ids_df = con.execute(
        '''SELECT id FROM movies WHERE id NOT IN (SELECT DISTINCT movie_id FROM movie_crew)'''
    ).fetchdf()
    
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
        con.register('batch_df_view', batch_df)
        con.execute(f"INSERT INTO movie_crew ({crew_columns}) SELECT {crew_columns} FROM batch_df_view;")
        log_and_print(f"Movie Crew Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows)")

    save_checkpoint(processed_ids, 'movie_crew_checkpoint.pkl')
    check_and_remove_movie_crew_duplicates(con)

    if skipped_ids:
        log_and_print(f"Skipped {len(skipped_ids)} movie IDs for crew", level='warning')
        with open('movie_crew_skipped_ids.log', 'w') as f:
            for sid in skipped_ids:
                f.write(f"{sid}\n")

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
            
            if 'production_countries' in movie_info:
                movie_info['production_countries'] = str(movie_info['production_countries']) if isinstance(movie_info['production_countries'], list) else None
            
            if 'origin_country' in movie_info:
                movie_info['origin_country'] = movie_info['origin_country'] if isinstance(movie_info['origin_country'], list) else None
            
            if 'genres' in movie_info:
                movie_info['genres'] = str(movie_info['genres']) if isinstance(movie_info['genres'], list) else None
            
            if 'production_companies' in movie_info:
                movie_info['production_companies'] = str(movie_info['production_companies']) if isinstance(movie_info['production_companies'], list) else None
            
            if 'spoken_languages' in movie_info:
                movie_info['spoken_languages'] = str(movie_info['spoken_languages']) if isinstance(movie_info['spoken_languages'], list) else None
            
            if 'belongs_to_collection' in movie_info:
                movie_info['belongs_to_collection'] = str(movie_info['belongs_to_collection']) if movie_info['belongs_to_collection'] else None
            
            return movie_info
        except HTTPError as e:
            if e.response.status_code == 404:
                return None
            log_and_print(f"HTTPError for movie ID {movie_id}: {e}", level='error')
            return None
        except Exception as e:
            log_and_print(f"Error fetching movie ID {movie_id}: {e}", level='error')
            return None


def check_and_remove_movie_duplicates(con):
    """Check for and remove duplicate movie rows."""
    log_and_print("Checking for movie duplicates...")
    
    dup_count = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT id
            FROM movies
            GROUP BY id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    
    if dup_count == 0:
        log_and_print("No movie duplicates found.")
        return
    
    log_and_print(f"Found {dup_count} movie duplicate IDs. Removing...")
    
    con.execute("""
        CREATE OR REPLACE TEMP TABLE movies_dedup AS
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn
            FROM movies
        )
        WHERE rn = 1
    """)
    
    before_count = con.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
    
    con.execute("DELETE FROM movies")
    con.execute("""
        INSERT INTO movies
        SELECT id, adult, backdrop_path, belongs_to_collection, budget, genres,
               homepage, imdb_id, origin_country, original_language, original_title,
               overview, popularity, poster_path, production_companies, production_countries,
               release_date, revenue, runtime, spoken_languages, status, tagline,
               title, video, vote_average, vote_count
        FROM movies_dedup
    """)
    
    after_count = con.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
    deleted = before_count - after_count
    
    con.execute("DROP TABLE IF EXISTS movies_dedup")
    log_and_print(f"Movie deduplication complete. Removed {deleted} duplicate rows.")


def update_movies(con):
    """Update movies data."""
    log_and_print("=" * 60)
    log_and_print("STARTING MOVIES INFO UPDATE")
    log_and_print("=" * 60)
    
    start_time = time.time()
    all_movie_data = []
    processed_count = 0
    skipped_ids = []
    
    checkpoint = load_checkpoint('movies_checkpoint.pkl')
    processed_ids = checkpoint if checkpoint else set()
    
    # Add columns if they don't exist
    columns_to_add = [
        "adult BOOLEAN",
        "backdrop_path VARCHAR",
        "belongs_to_collection VARCHAR",
        "budget BIGINT",
        "genres VARCHAR",
        "homepage VARCHAR",
        "imdb_id VARCHAR",
        "origin_country VARCHAR[]",
        "original_language VARCHAR",
        "original_title VARCHAR",
        "overview VARCHAR",
        "popularity DOUBLE",
        "poster_path VARCHAR",
        "production_companies VARCHAR",
        "production_countries VARCHAR",
        "release_date DATE",
        "revenue BIGINT",
        "runtime INTEGER",
        "spoken_languages VARCHAR",
        "status VARCHAR",
        "tagline VARCHAR",
        "title VARCHAR",
        "video BOOLEAN",
        "vote_average DOUBLE",
        "vote_count INTEGER"
    ]
    
    for col in columns_to_add:
        try:
            con.execute(f"ALTER TABLE movies ADD COLUMN IF NOT EXISTS {col};")
        except Exception:
            pass
    
    movies_ids_df = con.execute('''
        SELECT id FROM movies WHERE title IS NULL OR title = ''
    ''').fetchdf()
    
    if movies_ids_df.empty:
        log_and_print("No movies need updating.")
        check_and_remove_movie_duplicates(con)
        return
    
    movie_ids_to_process = [mid for mid in movies_ids_df['id'].tolist() if mid not in processed_ids]
    total_to_process = len(movie_ids_to_process)
    
    if total_to_process == 0:
        log_and_print("All movies already processed (from checkpoint).")
        check_and_remove_movie_duplicates(con)
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
                    save_checkpoint(processed_ids, 'movies_checkpoint.pkl')
                
            except Exception as e:
                log_and_print(f"Error processing movie ID {movie_id}: {e}", level='error')
                skipped_ids.append(movie_id)
    
    elapsed = time.time() - start_time
    log_and_print(f"Movie info API retrieval complete in {elapsed:.2f}s")
    log_and_print(f"Successfully fetched: {len(all_movie_data)}, Skipped: {len(skipped_ids)}")
    
    if not all_movie_data:
        log_and_print("No movie data to update.")
        check_and_remove_movie_duplicates(con)
        return
    
    movies_df = pd.DataFrame(all_movie_data)
    log_null_columns(movies_df, log_file='movies_null_columns.log')
    
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
    
    save_checkpoint(processed_ids, 'movies_checkpoint.pkl')
    check_and_remove_movie_duplicates(con)
    
    if skipped_ids:
        log_and_print(f"Skipped {len(skipped_ids)} movie IDs for info", level='warning')
        with open('movies_skipped_ids.log', 'w') as f:
            for sid in skipped_ids:
                f.write(f"{sid}\n")
    
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
    
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)
    
    try:
        # 1. Update TV show cast/crew
        update_tv_show_cast_crew(con)
        
        # 2. Update movie cast
        update_movie_cast(con)
        
        # 3. Update movie crew
        update_movie_crew(con)
        
        # 4. Update movies info
        update_movies(con)
        
    except Exception as e:
        log_and_print(f"Critical error in update job: {e}", level='error')
    finally:
        con.close()
    
    total_elapsed = time.time() - start_time
    hours, remainder = divmod(total_elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    log_and_print("=" * 60)
    log_and_print(f"FULL UPDATE JOB COMPLETE in {int(hours)}h {int(minutes)}m {seconds:.2f}s")
    log_and_print("=" * 60)


if __name__ == "__main__":
    run_update_job()
