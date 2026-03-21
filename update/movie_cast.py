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


def fetch_movie_cast(movie_id):
    for attempt in range(MAX_RETRIES):
        try:
            try:
                credits_dict = tmdb.Movies(movie_id).credits()
            except HTTPError as e:
                if e.response.status_code == 404:
                    return []
                if e.response.status_code == 429:
                    handle_rate_limit(attempt)
                    continue
                raise

            cast_list = credits_dict.get("cast", [])
            processed_cast_data = []

            for cast_member_dict in cast_list:  # No limit for cast
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

        except HTTPError as e:
            if e.response.status_code == 429:
                handle_rate_limit(attempt)
                continue
            log_and_print(f"HTTPError for movie cast {movie_id}: {e}", level='error')
            return []
        except Exception as e:
            log_and_print(f"Error fetching cast for movie ID {movie_id}: {e}", level='error')
            return []

    return []


MOVIE_CAST_PARTITION_COLS = ['movie_id', 'person_id', 'credit_id']
MOVIE_CAST_SELECT_COLS = [
    'movie_id', 'person_id', 'name', 'credit_id', 'character', 'cast_order',
    'gender', 'profile_path', 'known_for_department', 'popularity',
    'original_name', 'cast_id', 'inserted_at', 'updated_at'
]


def update_movie_cast(con):
    log_and_print("=" * 60)
    log_and_print("STARTING MOVIE CAST UPDATE")
    log_and_print("=" * 60)

    start_time = time.time()
    all_cast_data_flat = []
    processed_count = 0
    skipped_ids = []

    processed_ids = load_checkpoint('movie_cast_checkpoint.pkl')

    log_and_print('Fetching movie IDs from MotherDuck...')

    try:
        movies_ids_df = con.execute(
            '''SELECT id FROM movies WHERE id NOT IN (SELECT DISTINCT movie_id FROM movie_cast)'''
        ).fetchdf()
    except Exception:
        movies_ids_df = con.execute('SELECT id FROM movies').fetchdf()

    movie_ids_to_process = [mid for mid in movies_ids_df['id'].tolist() if mid not in processed_ids]
    movie_ids_to_process = apply_sample(movie_ids_to_process)
    total_to_process = len(movie_ids_to_process)
    log_and_print(f"Found {total_to_process} movie IDs to process.")

    if total_to_process == 0:
        log_and_print("No new movies to process for cast.")
        check_and_remove_duplicates(con, 'movie_cast', MOVIE_CAST_PARTITION_COLS, MOVIE_CAST_SELECT_COLS)
        return

    if DRY_RUN:
        log_and_print(f"[DRY RUN] Would fetch cast for {total_to_process} movies")
        return

    log_and_print(f'Starting parallel API retrieval with {MAX_API_WORKERS} workers...')

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
        future_to_movie_id = {
            executor.submit(fetch_movie_cast, movie_id): movie_id
            for movie_id in movie_ids_to_process
        }

        for future in as_completed(future_to_movie_id):
            movie_id = future_to_movie_id[future]
            try:
                cast_data = future.result()
                if cast_data:
                    all_cast_data_flat.extend(cast_data)
                else:
                    skipped_ids.append(movie_id)

                processed_ids.add(movie_id)
                processed_count += 1

                if processed_count % API_BATCH_SIZE == 0:
                    percent = (processed_count / total_to_process) * 100
                    log_and_print(f"Movie Cast Progress: {processed_count}/{total_to_process} ({percent:.2f}%)")
                    save_checkpoint(processed_ids, 'movie_cast_checkpoint.pkl')

            except Exception as e:
                log_and_print(f"Error processing movie ID {movie_id}: {e}", level='error')
                skipped_ids.append(movie_id)

    elapsed = time.time() - start_time
    log_and_print(f'Finished movie cast API retrieval in {elapsed:.2f}s')
    log_and_print(f'Total cast members fetched: {len(all_cast_data_flat)}')

    if not all_cast_data_flat:
        log_and_print("No movie cast data to insert.")
        check_and_remove_duplicates(con, 'movie_cast', MOVIE_CAST_PARTITION_COLS, MOVIE_CAST_SELECT_COLS)
        log_skipped_ids(skipped_ids, 'movie_cast_skipped_ids.log')
        return

    cast_df = pd.DataFrame(all_cast_data_flat)
    log_null_columns(cast_df, log_file='movie_cast_null_columns.log')

    log_and_print('Inserting movie cast data into MotherDuck...')
    cast_columns = 'movie_id, person_id, name, credit_id, character, cast_order, gender, profile_path, known_for_department, popularity, original_name, cast_id'

    num_batches = math.ceil(len(cast_df) / DB_INSERT_BATCH_SIZE)
    for i in range(num_batches):
        start_idx = i * DB_INSERT_BATCH_SIZE
        end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(cast_df))
        batch_df = cast_df.iloc[start_idx:end_idx]
        try:
            con.register('batch_df_view', batch_df)
            con.execute(f"INSERT INTO movie_cast ({cast_columns}) SELECT {cast_columns} FROM batch_df_view;")
            log_and_print(f"Movie Cast Batch {i+1}/{num_batches} inserted ({len(batch_df)} rows)")
        except Exception as e:
            log_and_print(f"Error inserting movie cast batch: {e}", level='error')

    save_checkpoint(processed_ids, 'movie_cast_checkpoint.pkl')
    check_and_remove_duplicates(con, 'movie_cast', MOVIE_CAST_PARTITION_COLS, MOVIE_CAST_SELECT_COLS)
    log_skipped_ids(skipped_ids, 'movie_cast_skipped_ids.log')

    total_elapsed = time.time() - start_time
    log_and_print(f'Movie cast update complete in {total_elapsed:.2f}s')
