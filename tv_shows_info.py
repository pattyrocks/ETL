import time
import math
import pandas as pd
import tmdbsimple as tmdb
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import HTTPError

from config import (
    DRY_RUN, MAX_API_WORKERS, DB_INSERT_BATCH_SIZE, API_BATCH_SIZE, MAX_RETRIES,
)
from utils import (
    log_and_print, handle_rate_limit, save_checkpoint, load_checkpoint,
    log_null_columns, log_skipped_ids, safe_str, apply_sample,
)
from dedup import check_and_remove_duplicates

TV_SHOW_PARTITION_COLS = ['id']
TV_SHOW_SELECT_COLS = [
    'id', 'name', 'episode_run_time',
    'in_production', 'popularity', 'last_air_date',
    'number_of_episodes', 'number_of_seasons',
    'origin_country', 'production_countries', 'status', 'type',
    'inserted_at', 'updated_at',
]


def fetch_tv_show_info(tv_id):
    for attempt in range(MAX_RETRIES):
        try:
            tv_info = tmdb.TV(tv_id).info()

            return {
                'id': tv_info.get('id'),
                'name': tv_info.get('name'),
                'popularity': tv_info.get('popularity'),
                'last_air_date': tv_info.get('last_air_date') or None,
                'episode_run_time': safe_str(tv_info.get('episode_run_time')),
                'in_production': tv_info.get('in_production'),
                'number_of_episodes': tv_info.get('number_of_episodes'),
                'number_of_seasons': tv_info.get('number_of_seasons'),
                'origin_country': safe_str(tv_info.get('origin_country')),
                'production_countries': safe_str(tv_info.get('production_countries')),
                'status': tv_info.get('status'),
                'type': tv_info.get('type'),
            }

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
    log_and_print("=" * 60)
    log_and_print("STARTING TV SHOWS INFO UPDATE")
    log_and_print("=" * 60)

    start_time = time.time()
    all_tv_data = []
    processed_count = 0
    skipped_ids = []

    processed_ids = load_checkpoint('tv_shows_info_checkpoint.pkl')

    tv_ids_df = con.execute('''
        SELECT id FROM tv_shows WHERE name IS NULL OR name = ''
    ''').fetchdf()

    if tv_ids_df.empty:
        log_and_print("No TV shows need info updating.")
        return

    tv_ids_to_process = [tid for tid in tv_ids_df['id'].tolist() if tid not in processed_ids]
    tv_ids_to_process = apply_sample(tv_ids_to_process)
    total_to_process = len(tv_ids_to_process)

    if total_to_process == 0:
        log_and_print("All TV shows already processed (from checkpoint).")
        return

    log_and_print(f"Starting TV show info retrieval for {total_to_process} shows...")

    if DRY_RUN:
        log_and_print(f"[DRY RUN] Would fetch info for {total_to_process} TV shows")
        return

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

    TV_SHOW_COLUMNS = ['id', 'name', 'episode_run_time',
        'in_production', 'popularity', 'last_air_date',
        'number_of_episodes', 'number_of_seasons',
        'origin_country', 'production_countries', 'status', 'type']
    tv_df = tv_df[[c for c in TV_SHOW_COLUMNS if c in tv_df.columns]]

    for date_col in ['last_air_date']:
        if date_col in tv_df.columns:
            tv_df[date_col] = tv_df[date_col].replace('', None)

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
                    popularity = batch_view.popularity,
                    last_air_date = batch_view.last_air_date,
                    episode_run_time = batch_view.episode_run_time,
                    in_production = batch_view.in_production,
                    number_of_episodes = batch_view.number_of_episodes,
                    number_of_seasons = batch_view.number_of_seasons,
                    origin_country = batch_view.origin_country,
                    production_countries = batch_view.production_countries,
                    status = batch_view.status,
                    type = batch_view.type,
                    updated_at = CURRENT_TIMESTAMP
                FROM batch_view
                WHERE tv_shows.id = batch_view.id
            ''')
            log_and_print(f"TV Show Info Batch {i+1}/{num_batches} updated ({len(batch_df)} rows)")
        except Exception as e:
            log_and_print(f"Error updating TV show batch: {e}", level='error')

    save_checkpoint(processed_ids, 'tv_shows_info_checkpoint.pkl')
    log_skipped_ids(skipped_ids, 'tv_shows_info_skipped_ids.log')

    check_and_remove_duplicates(con, 'tv_shows', TV_SHOW_PARTITION_COLS, TV_SHOW_SELECT_COLS)

    total_elapsed = time.time() - start_time
    log_and_print(f'TV shows info update complete in {total_elapsed:.2f}s')
