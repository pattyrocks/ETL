import tmdbsimple as tmdb
import os
import duckdb
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed # For parallel API calls
import math

tmdb.API_KEY = os.getenv('TMDBAPIKEY')
DATABASE_PATH = 'TMDB'

MAX_API_WORKERS = 15

DB_INSERT_BATCH_SIZE = None # None means insert all at once

def fetch_movie_credits(movie_id):
    """
    Fetches credits for a single movie ID.
    Includes error handling and returns a list of cast dictionaries with movie_id.
    Returns an empty list on failure.
    """
    try:
        credits_dict = tmdb.Movies(movie_id).credits()
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
    except Exception as e:
        print(f"Error fetching credits for movie ID {movie_id}: {e}")
        return []

def create_credits():
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)

    start_overall_time = time.time()
    
    # Using a list to gather all cast members from all movies
    all_cast_data_flat = []
    processed_movies_count = 0
    total_cast_members_count = 0

    print('Starting data retrieval of movie IDs from DuckDB...')
    # Consider only fetching IDs for movies that don't yet have cast data,
    # if you are resuming an interrupted process:
    movies_ids_df = con.execute('''SELECT id FROM movies WHERE id NOT IN (SELECT DISTINCT movie_id AS id FROM movie_cast)''').fetchdf()
    # movies_ids_df = con.execute('''SELECT id FROM movies LIMIT 100''').fetchdf()
    movie_ids_to_process = movies_ids_df['id'].tolist()
    total_movies_to_process = len(movie_ids_to_process)
    print(f"Found {total_movies_to_process} movie IDs to process.")

    if total_movies_to_process == 0:
        print("No movie IDs to process. Exiting.")
        con.close()
        return

    print(f'Starting parallel API retrieval with {MAX_API_WORKERS} workers...')
    start_api_fetch_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:

        futures = [executor.submit(fetch_movie_credits, movie_id) for movie_id in movie_ids_to_process]

        for i, future in enumerate(as_completed(futures)):
            movie_cast_data = future.result() # This is a list of dicts for one movie
            if movie_cast_data:
                all_cast_data_flat.extend(movie_cast_data) # Add all cast members to the flat list
                total_cast_members_count += len(movie_cast_data)
            processed_movies_count += 1

            if processed_movies_count % (total_movies_to_process // 10 or 1) == 0 or processed_movies_count == total_movies_to_process:
                elapsed_api_time = time.time() - start_api_fetch_time
                progress_percent = (processed_movies_count / total_movies_to_process) * 100
                print(f"Progress: {progress_percent:.1f}% | Processed {processed_movies_count}/{total_movies_to_process} movies | Fetched {total_cast_members_count} cast members | Elapsed API fetch time: {elapsed_api_time:.2f}s")
                # Optional: Add a small sleep here to respect rate limits if hitting them
                # time.sleep(0.01)

    end_api_fetch_time = time.time()
    print(f'Finished API retrieval in {end_api_fetch_time - start_api_fetch_time:.2f} seconds.')
    print(f'Total cast members fetched: {total_cast_members_count}')

    if not all_cast_data_flat:
        print("No cast data retrieved. Exiting.")
        con.close()
        return

    print('Starting to create DataFrame from all fetched data...')
    start_df_create_time = time.time()
    # Create the DataFrame once from the flat list of dictionaries
    cast_df = pd.DataFrame(all_cast_data_flat)

    end_df_create_time = time.time()
    print(f'DataFrame created in {end_df_create_time - start_df_create_time:.2f} seconds.')
    print("Sample of the consolidated DataFrame:")
    print(cast_df.head())
    print("DataFrame Info:")
    cast_df.info()

    print('Starting to create/insert into DuckDB table...')
    start_db_insert_time = time.time()

    try:
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS movie_cast (
            movie_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            character VARCHAR,
            cast_order INTEGER, -- cast_order is a keyword, so quote it
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

        # Add inserted_at and updated_at columns to existing table (idempotent)
        con.execute("ALTER TABLE movie_cast ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
        con.execute("ALTER TABLE movie_cast ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

        cast_columns = 'movie_id, person_id, name, credit_id, character, cast_order, gender, profile_path, known_for_department, popularity, original_name, cast_id'

        if DB_INSERT_BATCH_SIZE is None or len(cast_df) <= DB_INSERT_BATCH_SIZE:
            # Insert all at once if no batching is specified or DataFrame is small
            con.execute(f"INSERT INTO movie_cast ({cast_columns}) SELECT {cast_columns} FROM cast_df;")
            print(f"Inserted all {len(cast_df)} rows into 'movie_cast'.")
        else:
            # Batch insert if DataFrame is extremely large
            num_batches = math.ceil(len(cast_df) / DB_INSERT_BATCH_SIZE)
            print(f"Inserting in {num_batches} batches of {DB_INSERT_BATCH_SIZE} rows...")
            for i in range(num_batches):
                start_idx = i * DB_INSERT_BATCH_SIZE
                end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(cast_df))
                batch_df = cast_df.iloc[start_idx:end_idx]

                con.execute(f"INSERT INTO movie_cast ({cast_columns}) SELECT {cast_columns} FROM batch_df;") # DuckDB will see batch_df
                print(f"Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows).")

    except Exception as e:
        print(f"An error occurred during DB table creation/insertion: {e}")
        # Consider a rollback if inside a transaction for multiple inserts
        # con.execute("ROLLBACK;")
    finally:
        end_db_insert_time = time.time()
        print(f'DuckDB table created/inserted in {end_db_insert_time - start_db_insert_time:.2f} seconds.')
        con.close() # Close connection regardless of success/failure

    end_overall_time = time.time()
    elapsed_overall_time = end_overall_time - start_overall_time
    hours, remainder = divmod(elapsed_overall_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f'Overall process completed for {total_movies_to_process} movies, fetching {total_cast_members_count} cast members.')
    print(f'Total time elapsed: {int(hours)}h {int(minutes)}m {seconds:.2f}s')

    con.close()

create_credits()