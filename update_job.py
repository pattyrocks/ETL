import os
import time
import requests
import duckdb
import pandas as pd
from datetime import datetime, timezone, timedelta
import math
import argparse
import logging
import subprocess
import psutil

API_KEY = os.getenv('TMDBAPIKEY')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
MOTHERDUCK_DB = 'md:TMDB'
TMDB_BASE = 'https://api.themoviedb.org/3'
FETCH_BATCH_SIZE = 500  # Flush rows to DB every N processed items to guard against memory pressure

# --- logging setup ---
_log_file = f"etl_diagnostics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(_log_file),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

def log_and_print(message, level='info'):
    """Log to file and print message to console at the given level."""
    getattr(logger, level)(message)

def log_memory_usage(label=''):
    """Log current process memory usage."""
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024 / 1024
    tag = f" [{label}]" if label else ""
    log_and_print(f"Memory usage{tag}: {mem_mb:.1f} MB", level='debug')

# --- canonical column lists ---
MOVIES_COLS = [
    'id',
    'adult',
    'backdrop_path',
    'belongs_to_collection',
    'budget',
    'genres',
    'homepage',
    'imdb_id',
    'origin_country',
    'original_language',
    'original_title',
    'overview',
    'popularity',
    'poster_path',
    'production_companies',
    'production_countries',
    'release_date',
    'revenue',
    'runtime',
    'spoken_languages',
    'status',
    'tagline',
    'title',
    'video',
    'vote_average',
    'vote_count',
]

MOVIE_CAST_COLS = [
    'movie_id',
    'person_id',
    'name',
    'credit_id',
    'character',
    'cast_order',
    'gender',
    'profile_path',
    'known_for_department',
    'popularity',
    'original_name',
    'cast_id',
]

MOVIE_CREW_COLS = [
    'movie_id',
    'person_id',
    'name',
    'credit_id',
    'gender',
    'profile_path',
    'known_for_department',
    'popularity',
    'original_name',
    'adult',
    'department',
    'job',
]

TV_SHOWS_COLS = [
    'id',
    'episode_run_time',
    'homepage',
    'in_production',
    'last_air_date',
    'number_of_episodes',
    'number_of_seasons',
    'origin_country',
    'production_countries',
    'status',
    'type',
]

TV_CAST_COLS = [
    'tv_id',
    'person_id',
    'name',
    'credit_id',
    'character',
    'cast_order',
    'gender',
    'profile_path',
    'known_for_department',
    'popularity',
    'original_name',
    'roles',
    'total_episode_count',
    'cast_id',
    'also_known_as',
]

# --- connection helper ---
def get_connection():
    """Connect to MotherDuck."""
    if not MOTHERDUCK_TOKEN:
        raise ValueError("MOTHERDUCK_TOKEN environment variable not set")
    return duckdb.connect(MOTHERDUCK_DB)

# --- helpers ---
def iso_date(dt): 
    return dt.strftime('%Y-%m-%d')

def get_last_run(con):
    row = con.execute("""
        SELECT last_run FROM last_updates
        ORDER BY last_run DESC
        LIMIT 1
        """).fetchone()

    if row and row[0] is not None:
        try:
            return datetime.fromisoformat(row[0])
        except Exception:
            try:
                return datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                pass
    return datetime.now(timezone.utc) - timedelta(days=7)

def set_last_run(con, ts):
    con.execute("BEGIN")
    try:
        con.execute("DELETE FROM last_updates")
        con.execute("""
            INSERT INTO last_updates (last_run, inserted_at, updated_at)
            VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, [ts.isoformat()])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

def call_changes(endpoint, start_date, end_date, max_pages=1000):
    """
    Retrieve all pages from the TMDB /{endpoint}/changes endpoint between start_date and end_date.
    Returns a list of IDs found across all pages.
    """
    url = f"{TMDB_BASE}/{endpoint}/changes"
    page = 1
    ids = []
    call_start = time.time()
    while page <= max_pages:
        params = {
            'api_key': API_KEY,
            'start_date': iso_date(start_date),
            'end_date': iso_date(end_date),
            'page': page
        }
        try:
            req_start = time.time()
            resp = requests.get(url, params=params, timeout=30)
            req_elapsed = time.time() - req_start
            log_and_print(f"API call: {endpoint}/changes page {page} completed in {req_elapsed:.2f}s", level='debug')
            resp.raise_for_status()
            data = resp.json()
            results = data.get('results', [])
            ids.extend([r['id'] for r in results if 'id' in r])
            total_pages = data.get('total_pages') or data.get('total_pages', 1)
            if not results or page >= int(total_pages):
                break
            page += 1
            time.sleep(0.25)
        except requests.exceptions.HTTPError as he:
            status = getattr(he.response, 'status_code', None)
            log_and_print(f"HTTPError on changes {endpoint} page {page}: {he} (status {status})", level='error')
            if status == 429:
                log_and_print("Rate limited, sleeping 2s then retrying...", level='warning')
                time.sleep(2)
                continue
            break
        except Exception as e:
            log_and_print(f"Error calling changes {endpoint} page {page}: {e}", level='error')
            break
    total_elapsed = time.time() - call_start
    log_and_print(f"call_changes({endpoint}): retrieved {len(ids)} IDs across {page} page(s) in {total_elapsed:.2f}s")
    return ids

def _fetch_with_retry(url, params, retries=3, timeout=30):
    """Make an API GET request with retry logic, logging each attempt and response time."""
    for attempt in range(1, retries + 1):
        try:
            req_start = time.time()
            resp = requests.get(url, params=params, timeout=timeout)
            elapsed = time.time() - req_start
            log_and_print(f"API GET {url} attempt {attempt}/{retries}: HTTP {resp.status_code} in {elapsed:.2f}s", level='debug')
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            log_and_print(f"API call failed (attempt {attempt}/{retries}) for {url}: {e}", level='warning')
            if attempt < retries:
                sleep_time = 2 ** attempt
                log_and_print(f"Retrying in {sleep_time}s...", level='debug')
                time.sleep(sleep_time)
    log_and_print(f"API call failed after {retries} attempts: {url}", level='error')
    return None

def fetch_movie_detail_and_credits(movie_id):
    fetch_start = time.time()
    detail = _fetch_with_retry(f"{TMDB_BASE}/movie/{movie_id}", params={'api_key': API_KEY})
    credits = _fetch_with_retry(f"{TMDB_BASE}/movie/{movie_id}/credits", params={'api_key': API_KEY})
    elapsed = time.time() - fetch_start
    if detail is None or credits is None:
        log_and_print(f"Movie fetch error {movie_id}: one or more requests failed (total {elapsed:.2f}s)", level='error')
        return None, None
    log_and_print(f"Fetched movie {movie_id} detail+credits in {elapsed:.2f}s", level='debug')
    return detail, credits

def fetch_tv_detail_and_aggregate(tv_id):
    fetch_start = time.time()
    detail = _fetch_with_retry(f"{TMDB_BASE}/tv/{tv_id}", params={'api_key': API_KEY})
    agg = _fetch_with_retry(f"{TMDB_BASE}/tv/{tv_id}/aggregate_credits", params={'api_key': API_KEY, 'language': 'en-US'})
    elapsed = time.time() - fetch_start
    if detail is None or agg is None:
        log_and_print(f"TV fetch error {tv_id}: one or more requests failed (total {elapsed:.2f}s)", level='error')
        return None, None
    log_and_print(f"Fetched TV show {tv_id} detail+aggregate_credits in {elapsed:.2f}s", level='debug')
    return detail, agg


# --- ensure target tables exist with all columns ---
def ensure_tables(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS movies (
            id BIGINT PRIMARY KEY,
            title VARCHAR,
            release_date VARCHAR,
            original_language VARCHAR,
            popularity DOUBLE,
            vote_count INTEGER,
            adult BOOLEAN,
            backdrop_path VARCHAR,
            belongs_to_collection VARCHAR,
            budget BIGINT,
            genres VARCHAR,
            homepage VARCHAR,
            imdb_id VARCHAR,
            origin_country VARCHAR,
            original_title VARCHAR,
            overview VARCHAR,
            poster_path VARCHAR,
            production_companies VARCHAR,
            production_countries VARCHAR,
            revenue BIGINT,
            runtime INTEGER,
            spoken_languages VARCHAR,
            status VARCHAR,
            tagline VARCHAR,
            video BOOLEAN,
            vote_average DOUBLE,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS movie_cast (
            movie_id BIGINT,
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
            cast_id BIGINT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

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

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS tv_shows (
            id BIGINT PRIMARY KEY,
            episode_run_time VARCHAR,
            homepage VARCHAR,
            in_production BOOLEAN,
            last_air_date VARCHAR,
            number_of_episodes INTEGER,
            number_of_seasons INTEGER,
            origin_country VARCHAR,
            production_countries VARCHAR,
            status VARCHAR,
            type VARCHAR,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS tv_show_cast_crew (
            tv_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            character VARCHAR,
            "cast_order" INTEGER,
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

    # Migration: if last_updates has job_name column (old schema), drop and recreate it
    try:
        cols = [row[0] for row in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'last_updates'"
        ).fetchall()]
        if 'job_name' in cols:
            con.execute("DROP TABLE last_updates;")
    except Exception:
        pass  # Table doesn't exist yet — nothing to do

    con.execute("""
        CREATE TABLE IF NOT EXISTS last_updates (
            last_run TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Add inserted_at and updated_at columns to existing tables (idempotent)
    for table in ['movies', 'movie_cast', 'movie_crew', 'tv_shows', 'tv_show_cast_crew']:
        con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
        con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

def _timed_execute(con, sql, label=''):
    """Execute a SQL statement and log the elapsed time."""
    tag = f" [{label}]" if label else ""
    log_and_print(f"SQL{tag}: {sql.strip()}", level='debug')
    t0 = time.time()
    result = con.execute(sql)
    elapsed = time.time() - t0
    log_and_print(f"SQL{tag} completed in {elapsed:.3f}s", level='debug')
    return result

# --- upsert helpers ---
def upsert_table_from_rows(con, rows, table_name, canonical_cols, key_col=None, dry_run=False):
    """
    Upsert rows (list of dict) into table_name using canonical_cols ordering.
    Preserves inserted_at from existing rows, sets updated_at to current timestamp.
    If dry_run is True, print what would be done, skip DB writes, and write a summary CSV preview.
    """
    if not rows:
        return

    log_and_print(f"Upserting {len(rows)} rows into '{table_name}'...")
    log_memory_usage(f"before upsert {table_name}")

    df = pd.DataFrame(rows)
    for c in df.columns:
        df[c] = df[c].apply(lambda v: (str(v) if isinstance(v, (list, dict)) else v))

    # Special handling for movies: convert empty or invalid release_date to None
    if table_name == 'movies' and 'release_date' in df.columns:
        def clean_release_date(val):
            if pd.isna(val) or val == "":
                return None
            try:
                # Accepts YYYY-MM-DD, returns as is if valid
                pd.to_datetime(val, format="%Y-%m-%d", errors="raise")
                return val
            except Exception:
                return None
        df['release_date'] = df['release_date'].apply(clean_release_date)


    df_reindexed = df.reindex(columns=canonical_cols, fill_value=None)


    # Clean date columns for all tables (after reindexing)
    def clean_date(val):
        if pd.isna(val) or val == "":
            return None
        try:
            pd.to_datetime(val, format="%Y-%m-%d", errors="raise")
            return val
        except Exception:
            return None

    if table_name == 'movies' and 'release_date' in df_reindexed.columns:
        df_reindexed['release_date'] = df_reindexed['release_date'].apply(clean_date)

    if table_name == 'tv_shows':
        if 'last_air_date' in df_reindexed.columns:
            df_reindexed['last_air_date'] = df_reindexed['last_air_date'].apply(clean_date)

    # Determine the key column
    if key_col and key_col in df_reindexed.columns:
        key = key_col
    elif 'id' in df_reindexed.columns:
        key = 'id'
    elif 'movie_id' in df_reindexed.columns:
        key = 'movie_id'
    elif 'tv_id' in df_reindexed.columns:
        key = 'tv_id'
    else:
        key = None

    if dry_run:
        log_and_print(f"[DRY RUN] Would upsert {len(df_reindexed)} rows into {table_name}.")
        # Write summary CSV preview
        import csv
        preview_path = f"/tmp/update_job_{table_name}_preview_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(preview_path, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow([f"Table: {table_name}"])
            writer.writerow([f"Row count: {len(df_reindexed)}"])
            writer.writerow(["Columns:"] + list(df_reindexed.columns))
            writer.writerow([])
            writer.writerow(["Sample rows (up to 10):"])
            sample = df_reindexed.head(10)
            writer.writerow(list(sample.columns))
            for row in sample.itertuples(index=False):
                writer.writerow(list(row))
        log_and_print(f"[DRY RUN] Preview CSV written: {preview_path}")
        return

    con.register('tmp_df', df_reindexed)
    upsert_start = time.time()
    try:
        # Preserve old inserted_at values before delete
        if key:
            # Qualify key column for all SQL statements
            _timed_execute(con, f"""
                CREATE OR REPLACE TEMP TABLE old_inserted_at AS
                SELECT o.{key}, o.inserted_at FROM {table_name} o
                WHERE o.{key} IN (SELECT DISTINCT t.{key} FROM tmp_df t);
            """, label=f"save old inserted_at {table_name}")
            _timed_execute(con, f"DELETE FROM {table_name} WHERE {key} IN (SELECT DISTINCT t.{key} FROM tmp_df t);",
                           label=f"delete existing {table_name}")

        # Insert with preserved inserted_at (COALESCE to keep old value) and new updated_at
        insert_cols = ", ".join(canonical_cols)
        if key:
            # Qualify ambiguous columns in SELECT
            qualified_insert_cols = ", ".join([f"t.{col}" for col in canonical_cols])
            _timed_execute(con, f"""
                INSERT INTO {table_name} ({insert_cols}, inserted_at, updated_at)
                SELECT {qualified_insert_cols},
                       COALESCE(o.inserted_at, CURRENT_TIMESTAMP) AS inserted_at,
                       CURRENT_TIMESTAMP AS updated_at
                FROM tmp_df t
                LEFT JOIN old_inserted_at o ON t.{key} = o.{key};
            """, label=f"insert {table_name}")
        else:
            _timed_execute(con, f"INSERT INTO {table_name} ({insert_cols}, inserted_at, updated_at) SELECT {insert_cols}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM tmp_df;",
                           label=f"insert {table_name}")
    finally:
        try:
            con.unregister('tmp_df')
        except Exception:
            pass
        try:
            _timed_execute(con, "DROP TABLE IF EXISTS old_inserted_at;", label=f"cleanup {table_name}")
        except Exception:
            pass
    upsert_elapsed = time.time() - upsert_start
    log_and_print(f"Upsert '{table_name}' ({len(df_reindexed)} rows) completed in {upsert_elapsed:.3f}s")
    log_memory_usage(f"after upsert {table_name}")

def _format_seconds(s: float) -> str:
    hrs, rem = divmod(s, 3600)
    mins, secs = divmod(rem, 60)
    return f"{int(hrs)}h {int(mins)}m {secs:.2f}s"

SNAPSHOT_SAMPLE_ROWS = 1000  # Max rows to fetch per snapshot to limit memory usage

def save_snapshot(con, table_name, label=''):
    """Save a diagnostic sample snapshot of a database table to a CSV file.

    Only the first SNAPSHOT_SAMPLE_ROWS rows are fetched to avoid loading the
    full table into memory (which could be tens-of-thousands of rows for large
    tables like movies or movie_cast).
    """
    tag = label or datetime.now().strftime('%Y%m%d_%H%M%S')
    snapshot_path = f"/tmp/snapshot_{table_name}_{tag}.csv"
    try:
        total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        log_and_print(
            f"Snapshot: '{table_name}' has {total_rows} total rows; "
            f"saving sample of up to {SNAPSHOT_SAMPLE_ROWS} rows to {snapshot_path}"
        )
        df = con.execute(f"SELECT * FROM {table_name} LIMIT {SNAPSHOT_SAMPLE_ROWS}").fetchdf()
        df.to_csv(snapshot_path, index=False)
        log_and_print(f"Snapshot of '{table_name}' ({len(df)}/{total_rows} rows) saved to {snapshot_path}")
    except Exception as e:
        log_and_print(f"Failed to save snapshot for '{table_name}': {e}", level='error')

def run(sample_only=0, force_days=None, dry_run=False, time_budget=0):
    run_start = time.time()
    if time_budget > 0:
        log_and_print(f"ETL job starting with time budget of {_format_seconds(time_budget)}. Log file: {_log_file}")
    else:
        log_and_print(f"ETL job starting. Log file: {_log_file}")
    log_memory_usage("job start")

    # Connect to MotherDuck
    conn_start = time.time()
    con = get_connection()
    log_and_print(f"✓ Connected to MotherDuck in {time.time() - conn_start:.2f}s")

    # Ensure tables exist
    ensure_tables(con)

    if dry_run:
        log_and_print("\n--- DRY RUN: No database writes will be performed ---")
    else:
        log_and_print("\n--- Live run: TMDB_backup mirror created by backup_to_glacier.py before this step ---")


    # Determine last_run
    if force_days is not None:
        last_run = datetime.now(timezone.utc) - timedelta(days=force_days)
        log_and_print(f"Forcing update: fetching changes from last {force_days} days")
    else:
        last_run = get_last_run(con)
    now = datetime.now(timezone.utc)
    log_and_print(f"Last run: {last_run.isoformat()}, now: {now.isoformat()}")

    movie_ids = call_changes('movie', last_run, now)
    tv_ids = call_changes('tv', last_run, now)

    if sample_only and sample_only > 0:
        movie_ids = movie_ids[:sample_only]
        tv_ids = tv_ids[:sample_only]

    total_changes = len(movie_ids) + len(tv_ids)
    log_and_print(f"Found {len(movie_ids)} changed movies, {len(tv_ids)} changed tv shows (total changes: {total_changes})")

    # Fetch details phase
    fetch_start = time.time()
    budget_exceeded = False
    movies_rows, movie_cast_rows, movie_crew_rows = [], [], []
    processed_movie_count = 0
    milestones = {int(len(movie_ids) * p) for p in [0.25, 0.50, 0.75, 1.0]}
    movie_fetch_start = time.time()
    for i, mid in enumerate(movie_ids):
        if i in milestones and i > 0:
            pct = round(i / len(movie_ids) * 100)
            elapsed = time.time() - movie_fetch_start
            forecast_total = (elapsed / i) * len(movie_ids)
            remaining = forecast_total - elapsed
            log_and_print(
                f"  Movies: {pct}% ({i}/{len(movie_ids)} of {len(movie_ids)} total to upsert) | "
                f"elapsed: {_format_seconds(elapsed)} | "
                f"remaining: ~{_format_seconds(remaining)} | "
                f"ETA: {(datetime.now() + timedelta(seconds=remaining)).strftime('%H:%M:%S UTC')}"
            )
            log_memory_usage(f"movies fetch {pct}%")
        detail, credits = fetch_movie_detail_and_credits(mid)
        if not detail:
            continue
        detail_row = {k: (str(v) if isinstance(v, (list, dict)) else v) for k,v in detail.items()}
        detail_row.setdefault('id', mid)
        movies_rows.append({k: detail_row.get(k) for k in MOVIES_COLS if k in MOVIES_COLS})
        for c in credits.get('cast', []):
            movie_cast_rows.append({
                'movie_id': mid,
                'person_id': c.get('id'),
                'name': c.get('name'),
                'credit_id': c.get('credit_id'),
                'character': c.get('character'),
                'cast_order': c.get('cast_order'),
                'gender': c.get('gender'),
                'profile_path': c.get('profile_path'),
                'known_for_department': c.get('known_for_department'),
                'popularity': c.get('popularity'),
                'original_name': c.get('original_name'),
                'cast_id': c.get('cast_id')
            })
        for crew in credits.get('crew', []):
            movie_crew_rows.append({
                'movie_id': mid,
                'person_id': crew.get('id'),
                'name': crew.get('name'),
                'credit_id': crew.get('credit_id'),
                'gender': crew.get('gender'),
                'profile_path': crew.get('profile_path'),
                'known_for_department': crew.get('known_for_department'),
                'popularity': crew.get('popularity'),
                'original_name': crew.get('original_name'),
                'adult': crew.get('adult'),
                'department': crew.get('department'),
                'job': crew.get('job')
            })
        processed_movie_count += 1
        time.sleep(0.12)

        # Check time budget before continuing
        if time_budget > 0 and (time.time() - run_start) >= time_budget:
            log_and_print(
                f"⚠️ Time budget of {_format_seconds(time_budget)} reached after {processed_movie_count} movies "
                f"({processed_movie_count}/{len(movie_ids)} processed) — stopping fetch phase early.",
                level='warning'
            )
            budget_exceeded = True
            # Flush any accumulated rows before exiting the loop
            if not dry_run and movies_rows:
                log_and_print(f"Flushing {len(movies_rows)} remaining movie rows due to time budget cutoff")
                upsert_table_from_rows(con, movies_rows, 'movies', MOVIES_COLS, key_col='id', dry_run=dry_run)
                upsert_table_from_rows(con, movie_cast_rows, 'movie_cast', MOVIE_CAST_COLS, key_col='movie_id', dry_run=dry_run)
                upsert_table_from_rows(con, movie_crew_rows, 'movie_crew', MOVIE_CREW_COLS, key_col='movie_id', dry_run=dry_run)
                movies_rows, movie_cast_rows, movie_crew_rows = [], [], []
            break

        # Process and flush in batches to guard against memory pressure
        if not dry_run and processed_movie_count % FETCH_BATCH_SIZE == 0:
            log_and_print(f"Batch flush: upserting rows after processing {processed_movie_count} movie records")
            log_memory_usage(f"before batch flush at movie {processed_movie_count}")
            upsert_table_from_rows(con, movies_rows, 'movies', MOVIES_COLS, key_col='id', dry_run=dry_run)
            upsert_table_from_rows(con, movie_cast_rows, 'movie_cast', MOVIE_CAST_COLS, key_col='movie_id', dry_run=dry_run)
            upsert_table_from_rows(con, movie_crew_rows, 'movie_crew', MOVIE_CREW_COLS, key_col='movie_id', dry_run=dry_run)
            movies_rows, movie_cast_rows, movie_crew_rows = [], [], []
            log_memory_usage(f"after batch flush at movie {processed_movie_count}")

    tv_rows, tv_cast_rows = [], []
    processed_tv_count = 0
    milestones = {int(len(tv_ids) * p) for p in [0.25, 0.50, 0.75, 1.0]}
    tv_fetch_start = time.time()
    for i, tid in enumerate(tv_ids):
        if budget_exceeded:
            break
        if i in milestones and i > 0:
            pct = round(i / len(tv_ids) * 100)
            elapsed = time.time() - tv_fetch_start
            forecast_total = (elapsed / i) * len(tv_ids)
            remaining = forecast_total - elapsed
            log_and_print(
                f"  TV shows: {pct}% ({i}/{len(tv_ids)} of {len(tv_ids)} total to upsert) | "
                f"elapsed: {_format_seconds(elapsed)} | "
                f"remaining: ~{_format_seconds(remaining)} | "
                f"ETA: {(datetime.now() + timedelta(seconds=remaining)).strftime('%H:%M:%S UTC')}"
            )
            log_memory_usage(f"tv fetch {pct}%")
        detail, agg = fetch_tv_detail_and_aggregate(tid)
        if not detail:
            continue
        detail_row = {k: (str(v) if isinstance(v, (list, dict)) else v) for k,v in detail.items()}
        detail_row.setdefault('id', tid)
        tv_rows.append({k: detail_row.get(k) for k in TV_SHOWS_COLS if k in TV_SHOWS_COLS})
        for c in agg.get('cast', []):
            tv_cast_rows.append({
                'tv_id': tid,
                'person_id': c.get('id'),
                'name': c.get('name'),
                'credit_id': c.get('credit_id'),
                'character': c.get('character'),
                'cast_order': c.get('cast_order'),
                'gender': c.get('gender'),
                'profile_path': c.get('profile_path'),
                'known_for_department': c.get('known_for_department'),
                'popularity': c.get('popularity'),
                'original_name': c.get('original_name'),
                'roles': str(c.get('roles')),
                'total_episode_count': c.get('total_episode_count'),
                'cast_id': c.get('cast_id'),
                'also_known_as': str(c.get('also_known_as')) if c.get('also_known_as') else None
            })
        processed_tv_count += 1
        time.sleep(0.12)

        # Check time budget before continuing
        if time_budget > 0 and (time.time() - run_start) >= time_budget:
            log_and_print(
                f"⚠️ Time budget of {_format_seconds(time_budget)} reached after {processed_tv_count} TV shows "
                f"({processed_tv_count}/{len(tv_ids)} processed) — stopping fetch phase early.",
                level='warning'
            )
            budget_exceeded = True
            # Flush any accumulated rows before exiting the loop
            if not dry_run and tv_rows:
                log_and_print(f"Flushing {len(tv_rows)} remaining TV show rows due to time budget cutoff")
                upsert_table_from_rows(con, tv_rows, 'tv_shows', TV_SHOWS_COLS, key_col='id', dry_run=dry_run)
                upsert_table_from_rows(con, tv_cast_rows, 'tv_show_cast_crew', TV_CAST_COLS, key_col='tv_id', dry_run=dry_run)
                tv_rows, tv_cast_rows = [], []
            break

        # Process and flush in batches to guard against memory pressure
        if not dry_run and processed_tv_count % FETCH_BATCH_SIZE == 0:
            log_and_print(f"Batch flush: upserting rows after processing {processed_tv_count} TV show records")
            log_memory_usage(f"before batch flush at tv {processed_tv_count}")
            upsert_table_from_rows(con, tv_rows, 'tv_shows', TV_SHOWS_COLS, key_col='id', dry_run=dry_run)
            upsert_table_from_rows(con, tv_cast_rows, 'tv_show_cast_crew', TV_CAST_COLS, key_col='tv_id', dry_run=dry_run)
            tv_rows, tv_cast_rows = [], []
            log_memory_usage(f"after batch flush at tv {processed_tv_count}")

    # MotherDuck connection health check before upsert phase
    log_and_print("Checking MotherDuck connection before upsert phase...")
    try:
        con.execute("SELECT 1").fetchone()
        log_and_print("✓ MotherDuck connection alive — proceeding to upsert phase")
    except Exception as e:
        log_and_print(f"⚠️ MotherDuck connection was stale ({e}) — reconnecting...", level='warning')
        try:
            con.close()
        except Exception:
            pass
        con = get_connection()
        log_and_print("✓ MotherDuck reconnected successfully")

    fetch_end = time.time()
    fetch_elapsed = fetch_end - fetch_start
    processed_count = processed_movie_count + processed_tv_count
    avg_per_item = fetch_elapsed / processed_count if processed_count > 0 else 0
    predicted_fetch_total = avg_per_item * total_changes

    log_and_print(f"Fetch phase: processed {processed_count} items in {_format_seconds(fetch_elapsed)} (avg {_format_seconds(avg_per_item)} per item)")
    if budget_exceeded:
        skipped = total_changes - processed_count
        log_and_print(
            f"⚠️ Partial run: processed {processed_count}/{total_changes} items "
            f"({skipped} skipped due to time budget). "
            f"last_run will be advanced to now so the next scheduled run picks up new changes.",
            level='warning'
        )
    elif sample_only and sample_only > 0 and processed_count < total_changes:
        log_and_print(f"Prediction: estimated full fetch time for {total_changes} items is {_format_seconds(predicted_fetch_total)} based on sample")

    log_memory_usage("before final upsert phase")

    # Upsert phase (remaining rows not yet flushed in batch)
    upsert_phase_start = time.time()

    upsert_table_from_rows(con, movies_rows, 'movies', MOVIES_COLS, key_col='id', dry_run=dry_run)
    upsert_table_from_rows(con, movie_cast_rows, 'movie_cast', MOVIE_CAST_COLS, key_col='movie_id', dry_run=dry_run)
    upsert_table_from_rows(con, movie_crew_rows, 'movie_crew', MOVIE_CREW_COLS, key_col='movie_id', dry_run=dry_run)
    upsert_table_from_rows(con, tv_rows, 'tv_shows', TV_SHOWS_COLS, key_col='id', dry_run=dry_run)
    upsert_table_from_rows(con, tv_cast_rows, 'tv_show_cast_crew', TV_CAST_COLS, key_col='tv_id', dry_run=dry_run)

    if not dry_run:
        set_last_run(con, now)
    else:
        log_and_print("[DRY RUN] Would update last_run in DB.")

    upsert_elapsed = time.time() - upsert_phase_start

    run_end = time.time()
    total_elapsed = run_end - run_start

    log_and_print(f"DB upsert phase took {_format_seconds(upsert_elapsed)}")
    log_and_print(f"Total run time: {_format_seconds(total_elapsed)}")
    log_memory_usage("job end")

    if sample_only and sample_only > 0 and processed_count > 0:
        estimated_total_run = predicted_fetch_total + upsert_elapsed
        log_and_print(f"Estimated total run time for full dataset ({total_changes} items): {_format_seconds(estimated_total_run)} (based on sample)")

    # Save diagnostic snapshots of critical tables (outside upsert timing to keep measurements clean)
    snapshot_tag = datetime.now().strftime('%Y%m%d_%H%M%S')
    for tbl in ['movies', 'movie_cast', 'tv_shows']:
        save_snapshot(con, tbl, label=snapshot_tag)

    con.close()
    if budget_exceeded:
        log_and_print(f"\n⚠️ Partial update complete (time budget reached). {processed_count}/{total_changes} items processed. Full diagnostics written to {_log_file}")
    else:
        log_and_print(f"\n✓ Update complete! Full diagnostics written to {_log_file}")

def backup_to_glacier():
    log_and_print("Starting backup to Glacier...")
    try:
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backup_to_glacier.py")
        subprocess.run(
            ["python", script_path],
            check=True,
        )
        log_and_print("Backup to Glacier completed successfully.")
    except subprocess.TimeoutExpired:
        log_and_print("Backup timed out after 30 minutes — possibly hit MotherDuck compute limit.", level='error')
        return False
    except subprocess.CalledProcessError as e:
        log_and_print(f"Backup to Glacier failed: {e}", level='error')
        return False
    return True

def run_update_with_backup(sample_only=0, force_days=None, dry_run=False, time_budget=0):
    # Backup runs as a separate Thursday workflow — skipped here
    log_and_print("Backup handled by separate scheduled workflow. Proceeding with update...")
    run(sample_only=sample_only, force_days=force_days, dry_run=dry_run, time_budget=time_budget)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TMDB weekly update job')
    parser.add_argument('--sample', type=int, default=0, help='Process only N changed ids (movies and tv) for quick testing')
    parser.add_argument('--force', type=int, default=None, metavar='DAYS',
                        help='Ignore stored last_run and fetch changes from DAYS ago (e.g., --force 30 for last 30 days, --force 7 for last week)')
    parser.add_argument('--dry-run', action='store_true', help='Run without writing to the database (for manual testing)')
    parser.add_argument('--time-budget', type=int, default=0, metavar='SECONDS',
                        help='Stop processing after this many seconds and flush what has been collected (0 = no limit). '
                             'Use to avoid being hard-killed by CI job time limits (e.g., 19800 for 5h30m).')
    args = parser.parse_args()

    run_update_with_backup(sample_only=args.sample, force_days=args.force, dry_run=args.dry_run, time_budget=args.time_budget)
