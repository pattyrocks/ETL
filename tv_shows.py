import tmdbsimple as tmdb
import os
import duckdb
import pandas as pd
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

tmdb.API_KEY = os.getenv('TMDBAPIKEY')

con = duckdb.connect(database='TMDB', read_only=False)

con.execute('''
    CREATE TABLE IF NOT EXISTS tv_shows (
        id INTEGER PRIMARY KEY,
        episode_run_time VARCHAR[],
        homepage VARCHAR,
        in_production BOOLEAN,
        last_air_date VARCHAR,
        number_of_episodes INTEGER,
        number_of_seasons INTEGER,
        origin_country VARCHAR[],
        production_countries VARCHAR,
        status VARCHAR,
        type VARCHAR
    );
''')

processed_count = 0
skipped_ids = []
start_time = time.time()

def fetch_tv_show_info(tv_show_id):
    try:
        tv_show_info = tmdb.TV(tv_show_id).info()
        df = pd.DataFrame.from_dict(tv_show_info, orient='index').transpose()
        selected_df = df[[
            'episode_run_time', 'homepage', 'id', 'in_production',
            'last_air_date', 'number_of_episodes', 'number_of_seasons',
            'origin_country', 'production_countries', 'status', 'type'
        ]].copy()  # <-- Add .copy() here

        # Convert 'production_countries' list of dicts to a JSON string for storage
        if 'production_countries' in selected_df.columns and not selected_df['production_countries'].empty:
            selected_df.loc[:, 'production_countries'] = selected_df['production_countries'].apply(
                lambda x: str(x) if isinstance(x, list) else None
            )
        else:
            selected_df.loc[:, 'production_countries'] = None

        if 'episode_run_time' in selected_df.columns and not selected_df['episode_run_time'].empty:
            selected_df.loc[:, 'episode_run_time'] = selected_df['episode_run_time'].apply(
                lambda x: [str(item) for item in x] if isinstance(x, list) else None
            )
        else:
            selected_df.loc[:, 'episode_run_time'] = None

        if 'origin_country' in selected_df.columns and not selected_df['origin_country'].empty:
            selected_df.loc[:, 'origin_country'] = selected_df['origin_country'].apply(
                lambda x: [str(item) for item in x] if isinstance(x, list) else None
            )
        else:
            selected_df.loc[:, 'origin_country'] = None

        return selected_df
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: HTTPError for TV show ID {tv_show_id}: {e}")
        return ('http', tv_show_id)
    except KeyError as e:
        print(f"ERROR: KeyError for TV show ID {tv_show_id} - Missing data: {e}. DataFrame conversion might fail.")
        return ('key', tv_show_id)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred for TV show ID {tv_show_id}: {e}")
        return ('other', tv_show_id)

def add_info_to_tv_shows_parallel(max_workers=8):
    global processed_count
    tv_show_ids_df = con.sql('''SELECT id FROM tv_shows''').fetchdf()

    if tv_show_ids_df.empty:
        print("No TV show IDs found in the 'tv_shows' table. Please populate it first.")
        return

    print("Starting TV show data retrieval...\n")

    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for tv_show_id in tv_show_ids_df['id']:
            futures.append(executor.submit(fetch_tv_show_info, tv_show_id))

        for future in as_completed(futures):
            result = future.result()
            if isinstance(result, tuple):
                # Error occurred, add to skipped_ids
                skipped_ids.append(result[1])
                continue
            selected_df = result
            try:
                # Make sure the DataFrame 'selected_df' has the same 'id' column
                con.execute(f'''
                    UPDATE tv_shows
                    SET
                        episode_run_time = selected_df.episode_run_time,
                        homepage = selected_df.homepage,
                        in_production = selected_df.in_production,
                        last_air_date = selected_df.last_air_date,
                        number_of_episodes = selected_df.number_of_episodes,
                        number_of_seasons = selected_df.number_of_seasons,
                        origin_country = selected_df.origin_country,
                        production_countries = selected_df.production_countries,
                        status = selected_df.status,
                        type = selected_df.type
                    FROM selected_df
                    WHERE tv_shows.id = selected_df.id
                ''')
                processed_count += 1
                end_time = time.time()
                elapsed_time = end_time - start_time
                minutes = int(elapsed_time // 60)
                remaining_seconds = elapsed_time % 60
                print(f'Successfully updated data for TV show ID {selected_df["id"].values[0]}. Processed {processed_count} records in {minutes}m{remaining_seconds:.2f}s')
            except Exception as e:
                print(f"ERROR: Could not update TV show ID {selected_df['id'].values[0]}: {e}")
                skipped_ids.append(selected_df['id'].values[0])
                continue

    print(f"\nProcessing complete.")
    print(f"Successfully processed {processed_count} TV shows.")

    if skipped_ids:
        print(f"Skipped IDs due to errors: {skipped_ids}")

add_info_to_tv_shows_parallel()

con.close()
