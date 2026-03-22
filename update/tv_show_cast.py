import time
import math
import pandas as pd
import tmdbsimple as tmdb
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import HTTPError

from update.config import (
    DRY_RUN, MAX_API_WORKERS, DB_INSERT_BATCH_SIZE, API_BATCH_SIZE, MAX_RETRIES,
)
from update.utils import (
    log_and_print, handle_rate_limit, save_checkpoint, load_checkpoint,
    log_null_columns, log_skipped_ids, apply_sample,
)
from update.dedup import check_and_remove_duplicates


def fetch_tv_show_cast(tv_id):
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

            for cast_member in cast_list:
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

        except HTTPError as e:
            if e.response.status_code == 429:
                handle_rate_limit(attempt)
                continue
            log_and_print(f"HTTPError for tv_id {tv_id}: {e}", level='error')
            return []
        except Exception as e:
            log_and_print(f"Error fetching cast for tv_id {tv_id}: {e}", level='error')
            return []

    return []


TV_CAST_PARTITION_COLS = ['tv_id', 'person_id', 'credit_id']
TV_CAST_SELECT_COLS = [
    'tv_id', 'person_id', 'name', 'credit_id', 'character', 'cast_order',
    'gender', 'profile_path', 'known_for_department', 'popularity',
    'original_name', 'roles', 'total_episode_count', 'cast_id',
    'also_known_as', 'inserted_at', 'updated_at'
]


def update_tv_show_cast(con):
    log_and_print("=" * 60)
    log_and_print("STARTING TV SHOW CAST UPDATE")
    log_and_print("=" * 60)

    start_time = time.time()
    all_cast_data_flat = []
    processed_tv_count = 0
    skipped_ids = []

    processed_ids = load_checkpoint('tv_cast_checkpoint.pkl')

    log_and_print('Fetching TV show IDs from MotherDuck...')

    try:
        tv_ids_df = con.execute(
            '''SELECT id FROM tv_shows WHERE id NOT IN (SELECT DISTINCT tv_id FROM tv_show_cast)'''
        ).fetchdf()
    except Exception:
        tv_ids_df = con.execute('SELECT id FROM tv_shows').fetchdf()

    tv_ids_to_process = [tid for tid in tv_ids_df['id'].tolist() if tid not in processed_ids]
    tv_ids_to_process = apply_sample(tv_ids_to_process)
    total_tv_to_process = len(tv_ids_to_process)
    log_and_print(f"Found {total_tv_to_process} TV show IDs to process.")

    if total_tv_to_process == 0:
        log_and_print("No new TV shows to process.")
        check_and_remove_duplicates(con, 'tv_show_cast', TV_CAST_PARTITION_COLS, TV_CAST_SELECT_COLS)
        return

    if DRY_RUN:
        log_and_print(f"[DRY RUN] Would fetch cast for {total_tv_to_process} TV shows")
        return

    log_and_print(f'Starting parallel API retrieval with {MAX_API_WORKERS} workers...')

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        future_to_tv_id = {
            executor.submit(fetch_tv_show_cast, tv_id): tv_id
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
                    log_and_print(f"TV Cast Progress: {processed_tv_count}/{total_tv_to_process} ({percent:.2f}%)")
                    save_checkpoint(processed_ids, 'tv_cast_checkpoint.pkl')
            except Exception as e:
                log_and_print(f"Error processing tv_id {tv_id}: {e}", level='error')
                skipped_ids.append(tv_id)

    elapsed = time.time() - start_time
    log_and_print(f'Finished TV cast API retrieval in {elapsed:.2f}s')
    log_and_print(f'Total TV cast members fetched: {len(all_cast_data_flat)}')

    if not all_cast_data_flat:
        log_and_print("No TV cast data to insert.")
        check_and_remove_duplicates(con, 'tv_show_cast', TV_CAST_PARTITION_COLS, TV_CAST_SELECT_COLS)
        log_skipped_ids(skipped_ids, 'tv_cast_skipped_ids.log')
        return

    cast_df = pd.DataFrame(all_cast_data_flat)
    log_null_columns(cast_df, log_file='tv_cast_null_columns.log')

    log_and_print('Inserting TV cast data into MotherDuck...')
    tv_columns = 'tv_id, person_id, name, credit_id, character, cast_order, gender, profile_path, known_for_department, popularity, original_name, roles, total_episode_count, cast_id, also_known_as'

    num_batches = math.ceil(len(cast_df) / DB_INSERT_BATCH_SIZE)
    for i in range(num_batches):
        start_idx = i * DB_INSERT_BATCH_SIZE
        end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(cast_df))
        batch_df = cast_df.iloc[start_idx:end_idx]
        try:
            con.register('batch_df_view', batch_df)
            con.execute(f"INSERT INTO tv_show_cast ({tv_columns}) SELECT {tv_columns} FROM batch_df_view;")
            log_and_print(f"TV Cast Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows)")
        except Exception as e:
            log_and_print(f"Error inserting TV cast batch: {e}", level='error')

    save_checkpoint(processed_ids, 'tv_cast_checkpoint.pkl')
    check_and_remove_duplicates(con, 'tv_show_cast', TV_CAST_PARTITION_COLS, TV_CAST_SELECT_COLS)
    log_skipped_ids(skipped_ids, 'tv_cast_skipped_ids.log')

    total_elapsed = time.time() - start_time
    log_and_print(f'TV show cast update complete in {total_elapsed:.2f}s')
