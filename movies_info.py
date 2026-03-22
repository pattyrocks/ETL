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
    log_null_columns, log_skipped_ids, safe_str, apply_sample, purge_dead_ids,
)
from dedup import check_and_remove_duplicates

MOVIE_PARTITION_COLS = ['id']
MOVIE_SELECT_COLS = [
    'id', 'adult', 'belongs_to_collection', 'budget', 'genres',
    'homepage', 'imdb_id', 'origin_country', 'original_language', 'original_title',
    'popularity', 'production_companies', 'production_countries', 'release_date',
    'revenue', 'runtime', 'spoken_languages', 'status', 'title',
    'vote_average', 'vote_count', 'inserted_at', 'updated_at',
]


def fetch_movie_info(movie_id):
    for attempt in range(MAX_RETRIES):
        try:
            movie_info = tmdb.Movies(movie_id).info()

            return {
                'id': movie_info.get('id'),
                'adult': movie_info.get('adult'),
                'belongs_to_collection': safe_str(movie_info.get('belongs_to_collection')),
                'budget': movie_info.get('budget'),
                'genres': safe_str(movie_info.get('genres')),
                'homepage': movie_info.get('homepage'),
                'imdb_id': movie_info.get('imdb_id'),
                'origin_country': movie_info.get('origin_country') if isinstance(movie_info.get('origin_country'), list) else None,
                'original_language': movie_info.get('original_language'),
                'original_title': movie_info.get('original_title'),
                'popularity': movie_info.get('popularity'),
                'production_companies': safe_str(movie_info.get('production_companies')),
                'production_countries': safe_str(movie_info.get('production_countries')),
                'release_date': movie_info.get('release_date') or None,
                'revenue': movie_info.get('revenue'),
                'runtime': movie_info.get('runtime'),
                'spoken_languages': safe_str(movie_info.get('spoken_languages')),
                'status': movie_info.get('status'),
                'title': movie_info.get('title'),
                'vote_average': movie_info.get('vote_average'),
                'vote_count': movie_info.get('vote_count'),
            }

        except HTTPError as e:
            if e.response.status_code == 404:
                return None
            if e.response.status_code == 429:
                handle_rate_limit(attempt)
                continue
            log_and_print(f"HTTPError for movie ID {movie_id}: {e}", level='error')
            return None
        except Exception as e:
            log_and_print(f"Error fetching movie ID {movie_id}: {e}", level='error')
            return None

    return None


def update_movies_info(con):
    log_and_print("=" * 60)
    log_and_print("STARTING MOVIES INFO UPDATE")
    log_and_print("=" * 60)

    start_time = time.time()
    all_movie_data = []
    processed_count = 0
    skipped_ids = []

    processed_ids = load_checkpoint('movies_info_checkpoint.pkl')

    movies_ids_df = con.execute('''
        SELECT id FROM movies WHERE title IS NULL OR title = ''
    ''').fetchdf()

    if movies_ids_df.empty:
        log_and_print("No movies need info updating.")
        return

    movie_ids_to_process = [mid for mid in movies_ids_df['id'].tolist() if mid not in processed_ids]
    movie_ids_to_process = apply_sample(movie_ids_to_process)
    total_to_process = len(movie_ids_to_process)

    if total_to_process == 0:
        log_and_print("All movies already processed (from checkpoint).")
        return

    log_and_print(f"Starting movie info retrieval for {total_to_process} movies...")

    if DRY_RUN:
        log_and_print(f"[DRY RUN] Would fetch info for {total_to_process} movies")
        return

    with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
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

                if processed_count % API_BATCH_SIZE == 0:
                    percent = (processed_count / total_to_process) * 100
                    log_and_print(f"Movie Info Progress: {processed_count}/{total_to_process} ({percent:.2f}%)")
                    save_checkpoint(processed_ids, 'movies_info_checkpoint.pkl')

            except Exception as e:
                log_and_print(f"Error processing movie ID {movie_id}: {e}", level='error')
                skipped_ids.append(movie_id)

    elapsed = time.time() - start_time
    log_and_print(f"Movie info API retrieval complete in {elapsed:.2f}s")
    log_and_print(f"Successfully fetched: {len(all_movie_data)}, Skipped: {len(skipped_ids)}")

    if not all_movie_data:
        log_and_print("No movie data to update.")
        log_skipped_ids(skipped_ids, 'movies_info_skipped_ids.log')
        purge_dead_ids(con, 'movies', skipped_ids)
        return

    movies_df = pd.DataFrame(all_movie_data)

    MOVIE_COLUMNS = ['id', 'adult', 'belongs_to_collection', 'budget', 'genres',
        'homepage', 'imdb_id', 'origin_country', 'original_language', 'original_title',
        'popularity', 'production_companies', 'production_countries', 'release_date',
        'revenue', 'runtime', 'spoken_languages', 'status', 'title',
        'vote_average', 'vote_count']
    movies_df = movies_df[[c for c in MOVIE_COLUMNS if c in movies_df.columns]]

    for date_col in ['release_date']:
        if date_col in movies_df.columns:
            movies_df[date_col] = movies_df[date_col].replace('', None)

    log_null_columns(movies_df, log_file='movies_info_null_columns.log')

    log_and_print("Updating movies in database...")

    num_batches = math.ceil(len(movies_df) / DB_INSERT_BATCH_SIZE)
    for i in range(num_batches):
        start_idx = i * DB_INSERT_BATCH_SIZE
        end_idx = min((i + 1) * DB_INSERT_BATCH_SIZE, len(movies_df))
        batch_df = movies_df.iloc[start_idx:end_idx]

        try:
            con.register('batch_view', batch_df)
            con.execute('''
                UPDATE movies
                SET
                    adult = batch_view.adult,
                    belongs_to_collection = batch_view.belongs_to_collection,
                    budget = batch_view.budget,
                    genres = batch_view.genres,
                    homepage = batch_view.homepage,
                    imdb_id = batch_view.imdb_id,
                    origin_country = batch_view.origin_country,
                    original_language = batch_view.original_language,
                    original_title = batch_view.original_title,
                    popularity = batch_view.popularity,
                    production_companies = batch_view.production_companies,
                    production_countries = batch_view.production_countries,
                    release_date = batch_view.release_date,
                    revenue = batch_view.revenue,
                    runtime = batch_view.runtime,
                    spoken_languages = batch_view.spoken_languages,
                    status = batch_view.status,
                    title = batch_view.title,
                    vote_average = batch_view.vote_average,
                    vote_count = batch_view.vote_count,
                    updated_at = CURRENT_TIMESTAMP
                FROM batch_view
                WHERE movies.id = batch_view.id
            ''')
            log_and_print(f"Movie Info Batch {i+1}/{num_batches} updated ({len(batch_df)} rows)")
        except Exception as e:
            log_and_print(f"Error updating movie batch: {e}", level='error')

    save_checkpoint(processed_ids, 'movies_info_checkpoint.pkl')
    log_skipped_ids(skipped_ids, 'movies_info_skipped_ids.log')
    purge_dead_ids(con, 'movies', skipped_ids)

    check_and_remove_duplicates(con, 'movies', MOVIE_PARTITION_COLS, MOVIE_SELECT_COLS)

    total_elapsed = time.time() - start_time
    log_and_print(f'Movies info update complete in {total_elapsed:.2f}s')
