import tmdbsimple as tmdb
import os
import duckdb
import pandas as pd
import time
import logging
import math
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
API_BATCH_SIZE = 500  # Report progress every N movies

# --- Logging setup ---
_log_file = f"movie_crew_diagnostics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
    getattr(logger, level)(message)


def save_checkpoint(processed_ids, filename='movie_crew_checkpoint.pkl'):
    """Save processed IDs to a checkpoint file."""
    import pickle
    with open(filename, 'wb') as f:
        pickle.dump(processed_ids, f)


def load_checkpoint(filename='movie_crew_checkpoint.pkl'):
    """Load processed IDs from a checkpoint file."""
    import pickle
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return set()


def log_null_columns(df, log_file='movie_crew_null_columns.log'):
    """Log columns with null values."""
    null_counts = df.isnull().sum()
    with open(log_file, 'w') as f:
        for col, count in null_counts.items():
            if count > 0:
                f.write(f"{col}: {count} nulls\n")
    log_and_print(f"Null column log written to {log_file}")


def fetch_movie_credits(movie_id):
    """
    Fetches credits (specifically crew) for a single movie ID from TMDB.
    Includes error handling and returns a list of dictionaries.
    Returns an empty list on failure.
    """
    try:
        try:
            credits_dict = tmdb.Movies(movie_id).credits()
        except HTTPError as e:
            if e.response.status_code == 404:
                # Movie not found, skip silently
                return []
            raise
        
        crew_list = credits_dict.get("crew", [])
        processed_crew_data = []
        
        # Limit crew to 50 per movie to avoid excessive data
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
        log_and_print(f"Error fetching credits for movie ID {movie_id}: {e}", level='error')
        return []


def check_and_remove_duplicates(con):
    """Check for and remove duplicate rows."""
    log_and_print("Checking for duplicate rows...")
    
    dup_count = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT movie_id, person_id, credit_id
            FROM movie_crew
            GROUP BY movie_id, person_id, credit_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    
    if dup_count == 0:
        log_and_print("No duplicates found.")
        return
    
    log_and_print(f"Found {dup_count} duplicate groups. Removing...")
    
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
    log_and_print(f"Deduplication complete. Removed {deleted} duplicate rows.")


def create_credits():
    """
    Orchestrates the fetching of movie crew data from TMDB APIs and
    inserting it into MotherDuck. Handles parallel API calls and
    robust database operations.
    """
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)

    start_overall_time = time.time()
    all_crew_data_flat = []
    processed_movies_count = 0
    total_crew_members_count = 0
    skipped_ids = []

    # Load checkpoint
    processed_ids = load_checkpoint()

    log_and_print('Starting data retrieval of movie IDs from MotherDuck...')
    movies_ids_df = con.execute(
        '''SELECT id FROM movies WHERE id NOT IN (SELECT DISTINCT movie_id FROM movie_crew)'''
    ).fetchdf()
    
    # Filter out already processed IDs from checkpoint
    movie_ids_to_process = [mid for mid in movies_ids_df['id'].tolist() if mid not in processed_ids]
    total_movies_to_process = len(movie_ids_to_process)
    log_and_print(f"Found {total_movies_to_process} movie IDs to process.")

    if total_movies_to_process == 0:
        log_and_print("No movie IDs to process.")
        check_and_remove_duplicates(con)
        con.close()
        return

    log_and_print(f'Starting parallel API retrieval with {MAX_API_WORKERS} workers...')
    start_api_fetch_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        future_to_movie_id = {
            executor.submit(fetch_movie_credits, movie_id): movie_id
            for movie_id in movie_ids_to_process
        }

        for future in as_completed(future_to_movie_id):
            movie_id = future_to_movie_id[future]
            try:
                movie_crew_data = future.result()
                if movie_crew_data:
                    all_crew_data_flat.extend(movie_crew_data)
                    total_crew_members_count += len(movie_crew_data)
                else:
                    skipped_ids.append(movie_id)
                
                processed_ids.add(movie_id)
                processed_movies_count += 1

                # Progress reporting every API_BATCH_SIZE
                if processed_movies_count % API_BATCH_SIZE == 0:
                    elapsed_api_time = time.time() - start_api_fetch_time
                    progress_percent = (processed_movies_count / total_movies_to_process) * 100
                    log_and_print(f"Progress: {progress_percent:.1f}% | Processed {processed_movies_count}/{total_movies_to_process} | Fetched {total_crew_members_count} crew members | Elapsed: {elapsed_api_time:.2f}s")
                    save_checkpoint(processed_ids)
                    
            except Exception as e:
                log_and_print(f"Error processing movie ID {movie_id}: {e}", level='error')
                skipped_ids.append(movie_id)

    end_api_fetch_time = time.time()
    log_and_print(f'Finished API retrieval in {end_api_fetch_time - start_api_fetch_time:.2f} seconds.')
    log_and_print(f'Total crew members fetched: {total_crew_members_count}')
    log_and_print(f'Skipped movies: {len(skipped_ids)}')

    if not all_crew_data_flat:
        log_and_print("No crew data retrieved.")
        check_and_remove_duplicates(con)
        con.close()
        return

    log_and_print('Creating DataFrame from fetched data...')
    crew_df = pd.DataFrame(all_crew_data_flat)
    
    # Log null columns
    log_null_columns(crew_df)

    log_and_print('Starting to insert into MotherDuck table...')
    start_db_insert_time = time.time()

    try:
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

        con.execute("ALTER TABLE movie_crew ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
        con.execute("ALTER TABLE movie_crew ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

        crew_columns = 'movie_id, person_id, name, credit_id, gender, profile_path, known_for_department, popularity, original_name, adult, department, job'

        # Batch insert
        num_batches = math.ceil(len(crew_df) / DB_INSERT_BATCH_SIZE)
        log_and_print(f"Inserting in {num_batches} batches of up to {DB_INSERT_BATCH_SIZE} rows...")
        
        for i in range(num_batches):
            start_idx = i * DB_INSERT_BATCH_SIZE
            end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(crew_df))
            batch_df = crew_df.iloc[start_idx:end_idx]

            con.register('batch_df_view', batch_df)
            con.execute(f"INSERT INTO movie_crew ({crew_columns}) SELECT {crew_columns} FROM batch_df_view;")
            log_and_print(f"Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows)")

    except Exception as e:
        log_and_print(f"Error during DB insertion: {e}", level='error')

    end_db_insert_time = time.time()
    log_and_print(f'Database insertion completed in {end_db_insert_time - start_db_insert_time:.2f} seconds.')

    # Save final checkpoint
    save_checkpoint(processed_ids)

    # Run deduplication
    check_and_remove_duplicates(con)

    # Log skipped IDs
    if skipped_ids:
        log_and_print(f"Skipped {len(skipped_ids)} movie IDs", level='warning')
        with open('movie_crew_skipped_ids.log', 'w') as f:
            for sid in skipped_ids:
                f.write(f"{sid}\n")

    con.close()

    end_overall_time = time.time()
    elapsed_overall_time = end_overall_time - start_overall_time
    hours, remainder = divmod(elapsed_overall_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    log_and_print(f'Overall process completed for {total_movies_to_process} movies, fetching {total_crew_members_count} crew members.')
    log_and_print(f'Total time elapsed: {int(hours)}h {int(minutes)}m {seconds:.2f}s')


if __name__ == "__main__":
    create_credits()
