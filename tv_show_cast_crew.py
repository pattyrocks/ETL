import os
import requests
import duckdb
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

API_KEY = os.getenv('TMDBAPIKEY')
DATABASE_PATH = 'TMDB'

MAX_API_WORKERS = 15
DB_INSERT_BATCH_SIZE = None  # None means insert all at once

def fetch_tv_show_credits(tv_id):
    """
    Fetches credits for the most recent season of a TV show using tmdbsimple.
    Returns a list of cast dictionaries with tv_id.
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
        # Find the most recent season (by air_date or season_number)
        seasons_sorted = sorted(
            [s for s in seasons if s.get('season_number') is not None],
            key=lambda s: (s.get('air_date') or '', s['season_number']),
            reverse=True
        )
        recent_season = seasons_sorted[0]
        season_number = recent_season['season_number']
        season = tmdb.TV_Seasons(tv_id, season_number)
        credits = season.credits()
        cast_list = credits.get('cast', [])
        processed_cast_data = []
        for cast_member_dict in cast_list:
            processed_cast_data.append({
                'tv_id': tv_id,
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
                'roles': str(cast_member_dict.get('roles')) if 'roles' in cast_member_dict else None,
                'total_episode_count': cast_member_dict.get('total_episode_count'),
                'cast_id': cast_member_dict.get('cast_id'),
                'also_known_as': str(cast_member_dict.get('also_known_as')) if 'also_known_as' in cast_member_dict else None
            })
        return processed_cast_data
    except Exception as e:
        print(f"Error fetching recent season credits for TV show ID {tv_id}: {e}")
        return []

def create_tv_show_cast():
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)

    start_overall_time = time.time()
    all_cast_data_flat = []
    processed_tv_count = 0
    total_cast_members_count = 0

    # print('Starting data retrieval of TV show IDs from DuckDB...')
    tv_ids_df = con.execute(
        '''SELECT id FROM tv_shows WHERE id NOT IN (SELECT DISTINCT tv_id FROM tv_show_cast)'''
    ).fetchdf()
    tv_ids_to_process = tv_ids_df['id'].tolist()
    total_tv_to_process = len(tv_ids_to_process)
    # print(f"Found {total_tv_to_process} TV show IDs to process.")

    if total_tv_to_process == 0:
        con.close()
        return

    # print(f'Starting parallel API retrieval with {MAX_API_WORKERS} workers...')
    start_api_fetch_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        futures = [executor.submit(fetch_tv_show_credits, tv_id) for tv_id in tv_ids_to_process]

        for i, future in enumerate(as_completed(futures)):
            tv_cast_data = future.result()
            if tv_cast_data:
                all_cast_data_flat.extend(tv_cast_data)
                total_cast_members_count += len(tv_cast_data)
            processed_tv_count += 1

            # Optionally keep a minimal progress log, or remove entirely

    # end_api_fetch_time = time.time()
    print(f'Finished API retrieval in {end_api_fetch_time - start_api_fetch_time:.2f} seconds.')
    print(f'Total cast members fetched: {total_cast_members_count}')

    if not all_cast_data_flat:
        con.close()
        return

    start_df_create_time = time.time()
    cast_df = pd.DataFrame(all_cast_data_flat)
    end_df_create_time = time.time()

    # --- LIMIT CREW MEMBERS PER SHOW TO 50 ---
    if not cast_df.empty:
        cast_mask = cast_df['known_for_department'] == 'Acting'
        crew_mask = ~cast_mask
        cast_part = cast_df[cast_mask]
        crew_part = cast_df[crew_mask]
        crew_limited = (
            crew_part.groupby('tv_id', group_keys=False)
            .apply(lambda g: g.head(50))
        )
        cast_df = pd.concat([cast_part, crew_limited], ignore_index=True)

    start_db_insert_time = time.time()

    try:
        con.execute(f"""
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

        # Add inserted_at and updated_at columns to existing table (idempotent)
        con.execute("ALTER TABLE tv_show_cast_crew ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
        con.execute("ALTER TABLE tv_show_cast_crew ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

        cast_columns = 'tv_id, person_id, name, credit_id, character, cast_order, gender, profile_path, known_for_department, popularity, original_name, roles, total_episode_count, cast_id, also_known_as'

        if DB_INSERT_BATCH_SIZE is None or len(cast_df) <= DB_INSERT_BATCH_SIZE:
            con.execute(f"INSERT INTO tv_show_cast_crew ({cast_columns}) SELECT {cast_columns} FROM cast_df;")
        else:
            num_batches = math.ceil(len(cast_df) / DB_INSERT_BATCH_SIZE)
            for i in range(num_batches):
                start_idx = i * DB_INSERT_BATCH_SIZE
                end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(cast_df))
                batch_df = cast_df.iloc[start_idx:end_idx]
                con.execute(f"INSERT INTO tv_show_cast_crew ({cast_columns}) SELECT {cast_columns} FROM batch_df;")

    except Exception as e:
        pass  # Optionally log error
    finally:
        end_db_insert_time = time.time()
        con.close()

    end_overall_time = time.time()

create_tv_show_cast()