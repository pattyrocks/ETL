import time
import gzip
import json
import requests

from update.config import DRY_RUN, DB_INSERT_BATCH_SIZE
from update.utils import log_and_print, apply_sample
from update.schema import ensure_movies_table, ensure_tv_shows_table


def download_tmdb_export(export_type):
    from datetime import datetime, timedelta

    base_url = "http://files.tmdb.org/p/exports"

    dates_to_try = [
        datetime.utcnow(),
        datetime.utcnow() - timedelta(days=1),
        datetime.utcnow() - timedelta(days=2),
    ]

    for date in dates_to_try:
        filename = f"{export_type}_{date.strftime('%m_%d_%Y')}.json.gz"
        url = f"{base_url}/{filename}"

        log_and_print(f"Trying to download: {url}")

        try:
            response = requests.get(url, timeout=120)

            if response.status_code == 200:
                log_and_print(f"Successfully downloaded: {filename} ({len(response.content) / 1024 / 1024:.2f} MB)")
                return response.content
            elif response.status_code == 404:
                log_and_print(f"Export not found: {filename}, trying older date...")
                continue
            else:
                log_and_print(f"HTTP {response.status_code} for {filename}", level='warning')
                continue

        except requests.exceptions.Timeout:
            log_and_print(f"Timeout downloading {filename}", level='warning')
            continue
        except requests.exceptions.RequestException as e:
            log_and_print(f"Error downloading {filename}: {e}", level='error')
            continue

    raise Exception(f"Could not download TMDB {export_type} export")


def parse_ids_export(gzipped_content):
    all_ids = set()

    try:
        decompressed = gzip.decompress(gzipped_content)
    except gzip.BadGzipFile as e:
        log_and_print(f"Error decompressing file: {e}", level='error')
        raise

    lines = decompressed.decode('utf-8').strip().split('\n')

    for line in lines:
        if not line.strip():
            continue
        try:
            data = json.loads(line)
            item_id = data.get('id')
            if item_id is not None:
                all_ids.add(int(item_id))
        except json.JSONDecodeError:
            continue

    return all_ids


def discover_new_movie_ids(con):
    log_and_print("=" * 60)
    log_and_print("DISCOVERING NEW MOVIE IDS")
    log_and_print("=" * 60)

    start_time = time.time()

    try:
        if not DRY_RUN:
            ensure_movies_table(con)

        log_and_print("Fetching existing movie IDs from database...")
        try:
            result = con.execute("SELECT id FROM movies").fetchdf()
            existing_ids = set(result['id'].tolist()) if not result.empty else set()
        except Exception:
            log_and_print("movies table not found, treating as empty", level='warning')
            existing_ids = set()
        log_and_print(f"Found {len(existing_ids)} existing movie IDs")

        log_and_print("Downloading TMDB movie IDs export...")
        export_content = download_tmdb_export('movie_ids')
        all_tmdb_ids = parse_ids_export(export_content)
        log_and_print(f"Total movie IDs in TMDB: {len(all_tmdb_ids)}")

        new_ids = all_tmdb_ids - existing_ids
        log_and_print(f"New movie IDs to add: {len(new_ids)}")

        if not new_ids:
            log_and_print("No new movie IDs to add.")
            return

        new_ids_list = apply_sample(list(new_ids))

        if DRY_RUN:
            log_and_print(f"[DRY RUN] Would insert {len(new_ids_list)} new movie IDs")
            return

        inserted_count = 0
        for i in range(0, len(new_ids_list), DB_INSERT_BATCH_SIZE):
            batch = new_ids_list[i:i + DB_INSERT_BATCH_SIZE]
            values_str = ', '.join([f"({mid})" for mid in batch])
            try:
                con.execute(f"INSERT INTO movies (id) VALUES {values_str} ON CONFLICT (id) DO NOTHING")
                inserted_count += len(batch)
            except Exception as e:
                log_and_print(f"Error inserting movie batch: {e}", level='error')

            progress = min(i + DB_INSERT_BATCH_SIZE, len(new_ids_list))
            log_and_print(f"Movie ID Progress: {progress}/{len(new_ids_list)}")

        elapsed = time.time() - start_time
        log_and_print(f"Inserted {inserted_count} new movie IDs in {elapsed:.2f}s")

    except Exception as e:
        log_and_print(f"Error discovering movie IDs: {e}", level='error')
        import traceback
        log_and_print(traceback.format_exc(), level='error')


def discover_new_tv_show_ids(con):
    log_and_print("=" * 60)
    log_and_print("DISCOVERING NEW TV SHOW IDS")
    log_and_print("=" * 60)

    start_time = time.time()

    try:
        if not DRY_RUN:
            ensure_tv_shows_table(con)

        log_and_print("Fetching existing TV show IDs from database...")
        try:
            result = con.execute("SELECT id FROM tv_shows").fetchdf()
            existing_ids = set(result['id'].tolist()) if not result.empty else set()
        except Exception:
            log_and_print("tv_shows table not found, treating as empty", level='warning')
            existing_ids = set()
        log_and_print(f"Found {len(existing_ids)} existing TV show IDs")

        log_and_print("Downloading TMDB TV series IDs export...")
        export_content = download_tmdb_export('tv_series_ids')
        all_tmdb_ids = parse_ids_export(export_content)
        log_and_print(f"Total TV show IDs in TMDB: {len(all_tmdb_ids)}")

        new_ids = all_tmdb_ids - existing_ids
        log_and_print(f"New TV show IDs to add: {len(new_ids)}")

        if not new_ids:
            log_and_print("No new TV show IDs to add.")
            return

        new_ids_list = apply_sample(list(new_ids))

        if DRY_RUN:
            log_and_print(f"[DRY RUN] Would insert {len(new_ids_list)} new TV show IDs")
            return

        inserted_count = 0
        for i in range(0, len(new_ids_list), DB_INSERT_BATCH_SIZE):
            batch = new_ids_list[i:i + DB_INSERT_BATCH_SIZE]
            values_str = ', '.join([f"({tid})" for tid in batch])
            try:
                con.execute(f"INSERT INTO tv_shows (id) VALUES {values_str} ON CONFLICT (id) DO NOTHING")
                inserted_count += len(batch)
            except Exception as e:
                log_and_print(f"Error inserting TV show batch: {e}", level='error')

            progress = min(i + DB_INSERT_BATCH_SIZE, len(new_ids_list))
            log_and_print(f"TV Show ID Progress: {progress}/{len(new_ids_list)}")

        elapsed = time.time() - start_time
        log_and_print(f"Inserted {inserted_count} new TV show IDs in {elapsed:.2f}s")

    except Exception as e:
        log_and_print(f"Error discovering TV show IDs: {e}", level='error')
        import traceback
        log_and_print(traceback.format_exc(), level='error')
