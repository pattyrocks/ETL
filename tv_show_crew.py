import os
import duckdb
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

API_KEY = os.getenv('TMDBAPIKEY')
DATABASE_PATH = 'TMDB'

MAX_API_WORKERS = 15
DB_INSERT_BATCH_SIZE = None  # None means insert all at once

def fetch_tv_show_crew(tv_id):
    """
    Fetches crew for the most recent season of a TV show using tmdbsimple.
    Returns a list of crew dictionaries with tv_id.
    Returns an empty list on failure.
    """
    try:
        import tmdbsimple as tmdb
        tmdb.API_KEY = API_KEY
        tv = tmdb.TV(tv_id)
        tv_info = tv.info()
        seasons = tv_info.get('seasons', [])
        if not seasons:
            return []
        seasons_sorted = sorted(
            [s for s in seasons if s.get('season_number') is not None],
            key=lambda s: (s.get('air_date') or '', s['season_number']),
            reverse=True
        )
        recent_season = seasons_sorted[0]
        season_number = recent_season['season_number']
        season = tmdb.TV_Seasons(tv_id, season_number)
        credits = season.credits()
        crew_list = credits.get('crew', [])
        processed_crew_data = []
        # Limit crew to 50 per show to avoid excessive data
        for crew_member_dict in crew_list[:50]:
            processed_crew_data.append({
                'tv_id': tv_id,
                'person_id': crew_member_dict.get('id'),
                'name': crew_member_dict.get('name'),
                'credit_id': crew_member_dict.get('credit_id'),
                'gender': crew_member_dict.get('gender'),
                'profile_path': crew_member_dict.get('profile_path'),
                'known_for_department': crew_member_dict.get('known_for_department'),
                'popularity': crew_member_dict.get('popularity'),
                'original_name': crew_member_dict.get('original_name'),
                'department': crew_member_dict.get('department'),
                'job': crew_member_dict.get('job'),
            })
        return processed_crew_data
    except Exception as e:
        print(f"Error fetching crew for TV show ID {tv_id}: {e}")
        return []

def create_tv_show_crew():
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)

    start_overall_time = time.time()
    all_crew_data_flat = []
    processed_tv_count = 0
    total_crew_members_count = 0

    tv_ids_df = con.execute(
        '''SELECT id FROM tv_shows WHERE id NOT IN (SELECT DISTINCT tv_id FROM tv_show_crew)'''
    ).fetchdf()
    tv_ids_to_process = tv_ids_df['id'].tolist()
    total_tv_to_process = len(tv_ids_to_process)

    if total_tv_to_process == 0:
        print("No new TV shows to process.")
        con.close()
        return

    print(f"Found {total_tv_to_process} TV show IDs to process.")
    start_api_fetch_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        futures = [executor.submit(fetch_tv_show_crew, tv_id) for tv_id in tv_ids_to_process]

        for future in as_completed(futures):
            tv_crew_data = future.result()
            if tv_crew_data:
                all_crew_data_flat.extend(tv_crew_data)
                total_crew_members_count += len(tv_crew_data)
            processed_tv_count += 1

    end_api_fetch_time = time.time()
    print(f"Finished API retrieval in {end_api_fetch_time - start_api_fetch_time:.2f} seconds.")
    print(f"Total crew members fetched: {total_crew_members_count}")

    if not all_crew_data_flat:
        print("No crew data to insert.")
        con.close()
        return

    crew_df = pd.DataFrame(all_crew_data_flat)

    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS tv_show_crew (
            tv_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            department VARCHAR,
            job VARCHAR,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        con.execute("ALTER TABLE tv_show_crew ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
        con.execute("ALTER TABLE tv_show_crew ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

        crew_columns = 'tv_id, person_id, name, credit_id, gender, profile_path, known_for_department, popularity, original_name, department, job'

        if DB_INSERT_BATCH_SIZE is None or len(crew_df) <= DB_INSERT_BATCH_SIZE:
            con.register('crew_df_view', crew_df)
            con.execute(f"INSERT INTO tv_show_crew ({crew_columns}) SELECT {crew_columns} FROM crew_df_view;")
        else:
            num_batches = math.ceil(len(crew_df) / DB_INSERT_BATCH_SIZE)
            for i in range(num_batches):
                start_idx = i * DB_INSERT_BATCH_SIZE
                end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(crew_df))
                batch_df = crew_df.iloc[start_idx:end_idx]
                con.register('batch_df_view', batch_df)
                con.execute(f"INSERT INTO tv_show_crew ({crew_columns}) SELECT {crew_columns} FROM batch_df_view;")
                print(f"Inserted batch {i + 1}/{num_batches}")

    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        con.close()

    end_overall_time = time.time()
    print(f"Total job completed in {end_overall_time - start_overall_time:.2f} seconds.")

create_tv_show_crew()
