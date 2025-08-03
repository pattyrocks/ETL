import os
import requests
import duckdb
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

API_KEY = os.getenv('TMDBAPIKEY')
DATABASE_PATH = 'tmdb'

MAX_API_WORKERS = 15
DB_INSERT_BATCH_SIZE = None  # None means insert all at once

def fetch_tv_show_credits(tv_id):
    """
    Fetches aggregate credits for a single TV show ID using the TMDB API endpoint.
    Returns a list of cast dictionaries with tv_id.
    Returns an empty list on failure.
    """
    # not using tmdbsimple here, using requests directly because tmdbsimple only provides current season credits
    url = f"https://api.themoviedb.org/3/tv/{tv_id}/aggregate_credits?language=en-US"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        credits_dict = response.json()
        cast_list = credits_dict.get("cast", [])

        processed_cast_data = []
        for cast_member_dict in cast_list:
            processed_cast_data.append({
                'tv_id': tv_id,
                'person_id': cast_member_dict.get('id'),
                'name': cast_member_dict.get('name'),
                'credit_id': cast_member_dict.get('credit_id'),
                'character': cast_member_dict.get('character'),
                'order': cast_member_dict.get('order'),
                'gender': cast_member_dict.get('gender'),
                'profile_path': cast_member_dict.get('profile_path'),
                'known_for_department': cast_member_dict.get('known_for_department'),
                'popularity': cast_member_dict.get('popularity'),
                'original_name': cast_member_dict.get('original_name'),
                'roles': str(cast_member_dict.get('roles')) if 'roles' in cast_member_dict else None,
                'total_episode_count': cast_member_dict.get('total_episode_count'),
                'cast_id': cast_member_dict.get('cast_id'),  # may not always be present
                'also_known_as': str(cast_member_dict.get('also_known_as')) if 'also_known_as' in cast_member_dict else None
            })
        return processed_cast_data
    except Exception as e:
        print(f"Error fetching aggregate credits for TV show ID {tv_id}: {e}")
        return []

def create_tv_show_cast():
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)

    start_overall_time = time.time()
    all_cast_data_flat = []
    processed_tv_count = 0
    total_cast_members_count = 0

    print('Starting data retrieval of TV show IDs from DuckDB...')
    tv_ids_df = con.execute(
        '''SELECT id FROM tv_shows WHERE id NOT IN (SELECT DISTINCT tv_id FROM tv_show_cast)'''
    ).fetchdf()
    tv_ids_to_process = tv_ids_df['id'].tolist()
    total_tv_to_process = len(tv_ids_to_process)
    print(f"Found {total_tv_to_process} TV show IDs to process.")

    if total_tv_to_process == 0:
        print("No TV show IDs to process. Exiting.")
        con.close()
        return

    print(f'Starting parallel API retrieval with {MAX_API_WORKERS} workers...')
    start_api_fetch_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        futures = [executor.submit(fetch_tv_show_credits, tv_id) for tv_id in tv_ids_to_process]

        for i, future in enumerate(as_completed(futures)):
            tv_cast_data = future.result()
            if tv_cast_data:
                all_cast_data_flat.extend(tv_cast_data)
                total_cast_members_count += len(tv_cast_data)
            processed_tv_count += 1

            if processed_tv_count % (total_tv_to_process // 10 or 1) == 0 or processed_tv_count == total_tv_to_process:
                elapsed_api_time = time.time() - start_api_fetch_time
                progress_percent = (processed_tv_count / total_tv_to_process) * 100
                print(f"Progress: {progress_percent:.1f}% | Processed {processed_tv_count}/{total_tv_to_process} TV shows | Fetched {total_cast_members_count} cast members | Elapsed API fetch time: {elapsed_api_time:.2f}s")

    end_api_fetch_time = time.time()
    print(f'Finished API retrieval in {end_api_fetch_time - start_api_fetch_time:.2f} seconds.')
    print(f'Total cast members fetched: {total_cast_members_count}')

    if not all_cast_data_flat:
        print("No cast data retrieved. Exiting.")
        con.close()
        return

    print('Starting to create DataFrame from all fetched data...')
    start_df_create_time = time.time()
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
        CREATE OR REPLACE TABLE tv_show_cast_crew (
            tv_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            character VARCHAR,
            "order" INTEGER,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            roles VARCHAR,
            total_episode_count INTEGER,
            cast_id BIGINT,
            also_known_as VARCHAR
        );
        """)

        if DB_INSERT_BATCH_SIZE is None or len(cast_df) <= DB_INSERT_BATCH_SIZE:
            con.execute("INSERT INTO tv_show_cast_crew SELECT * FROM cast_df;")
            print(f"Inserted all {len(cast_df)} rows into 'tv_show_cast_crew'.")
        else:
            num_batches = math.ceil(len(cast_df) / DB_INSERT_BATCH_SIZE)
            print(f"Inserting in {num_batches} batches of {DB_INSERT_BATCH_SIZE} rows...")
            for i in range(num_batches):
                start_idx = i * DB_INSERT_BATCH_SIZE
                end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(cast_df))
                batch_df = cast_df.iloc[start_idx:end_idx]
                con.execute("INSERT INTO tv_show_cast_crew SELECT * FROM batch_df;")
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

    print(f'Overall process completed for {total_tv_to_process} TV shows, fetching {total_cast_members_count} cast members.')
    print(f'Total time elapsed: {int(hours)}h {int(minutes)}m {seconds:.2f}s')

create_tv_show_cast()