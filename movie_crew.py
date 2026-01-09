import tmdbsimple as tmdb
import os
import duckdb
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

tmdb.API_KEY = os.getenv('TMDBAPIKEY')

DATABASE_PATH = 'TMDB'

MAX_API_WORKERS = 15

DB_INSERT_BATCH_SIZE = None

def fetch_movie_credits(movie_id):
    """
    Fetches credits (specifically crew) for a single movie ID from TMDB.
    Includes error handling and returns a list of dictionaries, where each
    dictionary represents a crew member and includes the movie_id.
    Returns an empty list on failure to fetch or process.
    """
    try:
        credits_dict = tmdb.Movies(movie_id).credits()
        crew_list = credits_dict.get("crew", [])

        processed_crew_data = []
        for crew_member_dict in crew_list:
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

        print(f"Error fetching credits for movie ID {movie_id}: {e}")
        return []

def create_credits():
    """
    Orchestrates the fetching of movie crew data from TMDB APIs and
    inserting it into a DuckDB table. Handles parallel API calls and
    robust database operations.
    """
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)

    start_overall_time = time.time()
    
    all_crew_data_flat = []
    processed_movies_count = 0
    total_crew_members_count = 0

    print('Starting data retrieval of movie IDs from DuckDB...')
    # movies_ids_df = con.execute('''SELECT CAST(id AS STRING) AS id FROM movies WHERE id NOT IN (SELECT DISTINCT CAST(movie_id AS STRING) AS id FROM movie_crew)''').fetchdf()
    movies_ids_df = con.execute('''SELECT id FROM movies''').fetchdf()
    movie_ids_to_process = movies_ids_df['id'].tolist()
    total_movies_to_process = len(movie_ids_to_process)
    print(f"Found {total_movies_to_process} movie IDs to process.")

    if total_movies_to_process == 0:
        print("No movie IDs to process. Exiting.")
        con.close()
        return

    print(f'Starting parallel API retrieval with {MAX_API_WORKERS} workers...')
    start_api_fetch_time = time.time()

    # Use ThreadPoolExecutor for concurrent API calls to TMDB
    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:

        futures = [executor.submit(fetch_movie_credits, movie_id) for movie_id in movie_ids_to_process]


        for i, future in enumerate(as_completed(futures)):
            movie_crew_data = future.result() # This is a list of dicts for one movie
            if movie_crew_data:
                all_crew_data_flat.extend(movie_crew_data) # Add all crew members to the flat list
                total_crew_members_count += len(movie_crew_data)
            processed_movies_count += 1

            # Provide progress updates
            if processed_movies_count % (total_movies_to_process // 10 or 1) == 0 or processed_movies_count == total_movies_to_process:
                elapsed_api_time = time.time() - start_api_fetch_time
                progress_percent = (processed_movies_count / total_movies_to_process) * 100
                print(f"Progress: {progress_percent:.1f}% | Processed {processed_movies_count}/{total_movies_to_process} movies | Fetched {total_crew_members_count} crew members | Elapsed API fetch time: {elapsed_api_time:.2f}s")
                # Optional: Add a small sleep here to respect rate limits if hitting them often
                # time.sleep(0.01)

    end_api_fetch_time = time.time()
    print(f'Finished API retrieval in {end_api_fetch_time - start_api_fetch_time:.2f} seconds.')
    print(f'Total crew members fetched: {total_crew_members_count}')

    if not all_crew_data_flat:
        print("No crew data retrieved. Exiting.")
        con.close()
        return

    print('Starting to create DataFrame from all fetched data...')
    start_df_create_time = time.time()

    crew_df = pd.DataFrame(all_crew_data_flat)
    
    end_df_create_time = time.time()
    print(f'DataFrame created in {end_df_create_time - start_df_create_time:.2f} seconds.')
    print("Sample of the consolidated DataFrame:")
    print(crew_df.head())
    print("DataFrame Info:")
    crew_df.info()

    print('Starting to create/insert into DuckDB table...')
    start_db_insert_time = time.time()

    try:
        con.execute(f"""
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

        # Add inserted_at and updated_at columns to existing table (idempotent)
        con.execute("ALTER TABLE movie_crew ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
        con.execute("ALTER TABLE movie_crew ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
        print("Created/Replaced 'movie_crew' table with the specified schema.")

        columns_to_insert = [
            'movie_id', 'person_id', 'name', 'credit_id', 'gender',
            'profile_path', 'known_for_department', 'popularity',
            'original_name', 'adult', 'department', 'job'
        ]
        columns_sql = ", ".join(columns_to_insert)
        
        missing_df_columns = [col for col in columns_to_insert if col not in crew_df.columns]
        if missing_df_columns:
            print(f"Warning: The following columns are defined for insertion but missing from the DataFrame: {missing_df_columns}. They will be inserted as NULL.")
        
        crew_df_reordered = crew_df.reindex(columns=columns_to_insert, fill_value=None)


        if DB_INSERT_BATCH_SIZE is None or len(crew_df_reordered) <= DB_INSERT_BATCH_SIZE:
            # Insert all at once if no batching is specified or DataFrame is small
            # Use `__df_crew_df_reordered` for DuckDB to refer to the pandas DataFrame
            con.execute(f"INSERT INTO movie_crew ({columns_sql}) SELECT {columns_sql} FROM crew_df_reordered;")
            print(f"Inserted all {len(crew_df_reordered)} rows into 'movie_crew'.")
        else:
            # Batch insert if DataFrame is extremely large
            num_batches = math.ceil(len(crew_df_reordered) / DB_INSERT_BATCH_SIZE)
            print(f"Inserting in {num_batches} batches of {DB_INSERT_BATCH_SIZE} rows...")
            for i in range(num_batches):
                start_idx = i * DB_INSERT_BATCH_SIZE
                end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(crew_df_reordered))
                batch_df = crew_df_reordered.iloc[start_idx:end_idx]

                # Use `__df_batch_df` for DuckDB to refer to the pandas DataFrame
                con.execute(f"INSERT INTO movie_crew ({columns_sql}) SELECT {columns_sql} FROM batch_df;")
                print(f"Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows).")

    except Exception as e:
        print(f"An error occurred during DB table creation/insertion: {e}")

    finally:
        end_db_insert_time = time.time()
        print(f'DuckDB table created/inserted in {end_db_insert_time - start_db_insert_time:.2f} seconds.')
        con.close()

    end_overall_time = time.time()
    elapsed_overall_time = end_overall_time - start_overall_time
    hours, remainder = divmod(elapsed_overall_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f'Overall process completed for {total_movies_to_process} movies, fetching {total_crew_members_count} crew members.')
    print(f'Total time elapsed: {int(hours)}h {int(minutes)}m {seconds:.2f}s')

    con.close()

create_credits()
