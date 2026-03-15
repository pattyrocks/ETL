import tmdbsimple as tmdb
import os
import duckdb
import pandas as pd
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from requests.exceptions import HTTPError

# --- Configuration ---
API_KEY = os.getenv('TMDBAPIKEY')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
DATABASE_PATH = f'md:TMDB?motherduck_token={MOTHERDUCK_TOKEN}'

tmdb.API_KEY = API_KEY

MAX_API_WORKERS = 15
API_BATCH_SIZE = 500  # Report progress every N movies
DB_BATCH_SIZE = 1000  # Batch database updates

# --- Logging setup ---
_log_file = f"movies_diagnostics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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


def save_checkpoint(processed_ids, filename='movies_checkpoint.pkl'):
    """Save processed IDs to a checkpoint file."""
    import pickle
    with open(filename, 'wb') as f:
        pickle.dump(processed_ids, f)


def load_checkpoint(filename='movies_checkpoint.pkl'):
    """Load processed IDs from a checkpoint file."""
    import pickle
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return set()


def fetch_movie_info(movie_id):
    """
    Fetch movie info from TMDB API.
    Returns a dict with movie data or None on failure.
    """
    try:
        movie_info = tmdb.Movies(movie_id).info()
        
        # Convert complex fields to strings for storage
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
            # Movie not found, skip silently
            return None
        log_and_print(f"HTTPError for movie ID {movie_id}: {e}", level='error')
        return None
    except Exception as e:
        log_and_print(f"Error fetching movie ID {movie_id}: {e}", level='error')
        return None


def log_null_columns(df, log_file='movies_null_columns.log'):
    """Log columns with null values."""
    null_counts = df.isnull().sum()
    with open(log_file, 'w') as f:
        for col, count in null_counts.items():
            if count > 0:
                f.write(f"{col}: {count} nulls\n")
    log_and_print(f"Null column log written to {log_file}")


def check_and_remove_duplicates(con):
    """Check for and remove duplicate rows."""
    log_and_print("Checking for duplicate rows...")
    
    dup_count = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT id
            FROM movies
            GROUP BY id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    
    if dup_count == 0:
        log_and_print("No duplicates found.")
        return
    
    log_and_print(f"Found {dup_count} duplicate IDs. Removing...")
    
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
    log_and_print(f"Deduplication complete. Removed {deleted} duplicate rows.")


def add_info_to_movies_parallel(max_workers=MAX_API_WORKERS):
    """Main function to fetch and update movie info in parallel."""
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)
    
    start_time = time.time()
    all_movie_data = []
    processed_count = 0
    skipped_ids = []
    
    # Load checkpoint
    processed_ids = load_checkpoint()
    
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
        except Exception as e:
            log_and_print(f"Column already exists or error: {e}", level='debug')
    
    # Get movies that need updating (where title is NULL means not yet fetched)
    movies_ids_df = con.execute('''
        SELECT id FROM movies WHERE title IS NULL OR title = ''
    ''').fetchdf()
    
    if movies_ids_df.empty:
        log_and_print("No movies need updating.")
        check_and_remove_duplicates(con)
        con.close()
        return
    
    # Filter out already processed IDs
    movie_ids_to_process = [mid for mid in movies_ids_df['id'].tolist() if mid not in processed_ids]
    total_to_process = len(movie_ids_to_process)
    
    if total_to_process == 0:
        log_and_print("All movies already processed (from checkpoint).")
        check_and_remove_duplicates(con)
        con.close()
        return
    
    log_and_print(f"Starting movie data retrieval for {total_to_process} movies...")
    
    # Fetch movie data in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
                
                # Progress reporting every API_BATCH_SIZE
                if processed_count % API_BATCH_SIZE == 0:
                    elapsed = time.time() - start_time
                    percent = (processed_count / total_to_process) * 100
                    log_and_print(f"Progress: {processed_count}/{total_to_process} ({percent:.2f}%) - Elapsed: {elapsed:.2f}s")
                    save_checkpoint(processed_ids)
                
            except Exception as e:
                log_and_print(f"Error processing movie ID {movie_id}: {e}", level='error')
                skipped_ids.append(movie_id)
    
    # Final progress report
    elapsed = time.time() - start_time
    log_and_print(f"API retrieval complete. Processed {processed_count} movies in {elapsed:.2f}s")
    log_and_print(f"Successfully fetched: {len(all_movie_data)}, Skipped: {len(skipped_ids)}")
    
    if not all_movie_data:
        log_and_print("No movie data to update.")
        check_and_remove_duplicates(con)
        con.close()
        return
    
    # Create DataFrame and log null columns
    movies_df = pd.DataFrame(all_movie_data)
    log_null_columns(movies_df)
    
    # Batch update database
    log_and_print("Updating database in batches...")
    update_start = time.time()
    updated_count = 0
    
    for i in range(0, len(movies_df), DB_BATCH_SIZE):
        batch_df = movies_df.iloc[i:i + DB_BATCH_SIZE]
        
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
            updated_count += len(batch_df)
            log_and_print(f"Updated batch {i // DB_BATCH_SIZE + 1}/{(len(movies_df) // DB_BATCH_SIZE) + 1}")
        except Exception as e:
            log_and_print(f"Error updating batch: {e}", level='error')
    
    update_elapsed = time.time() - update_start
    log_and_print(f"Database update complete. Updated {updated_count} rows in {update_elapsed:.2f}s")
    
    # Save final checkpoint
    save_checkpoint(processed_ids)
    
    # Run deduplication
    check_and_remove_duplicates(con)
    
    # Log skipped IDs
    if skipped_ids:
        log_and_print(f"Skipped {len(skipped_ids)} IDs due to errors: {skipped_ids[:20]}...", level='warning')
        with open('movies_skipped_ids.log', 'w') as f:
            for sid in skipped_ids:
                f.write(f"{sid}\n")
    
    total_elapsed = time.time() - start_time
    log_and_print(f"\nTotal job completed in {total_elapsed:.2f}s")
    
    con.close()


if __name__ == "__main__":
    add_info_to_movies_parallel()