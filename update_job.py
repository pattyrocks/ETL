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

API_KEY = os.getenv('TMDBAPIKEY')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
MOTHERDUCK_DB = 'md:TMDB'
TMDB_BASE = 'https://api.themoviedb.org/3'

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

def get_last_run(con, job_name='weekly_update'):
    row = con.execute("""
        SELECT last_run FROM last_updates
        WHERE job_name = ?
        ORDER BY last_run DESC
        LIMIT 1
        """, [job_name]).fetchone()

    if row and row[0] is not None:
        try:
            return datetime.fromisoformat(row[0])
        except Exception:
            try:
                return datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                pass
    return datetime.now(timezone.utc) - timedelta(days=7)

def set_last_run(con, ts, job_name='weekly_update'):
    con.execute("""
        INSERT INTO last_updates (job_name, last_run, inserted_at, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """, [job_name, ts.isoformat()])

def call_changes(endpoint, start_date, end_date, max_pages=1000):
    """
    Retrieve all pages from the TMDB /{endpoint}/changes endpoint between start_date and end_date.
    Returns a list of IDs found across all pages.
    """
    url = f"{TMDB_BASE}/{endpoint}/changes"
    page = 1
    ids = []
    while page <= max_pages:
        params = {
            'api_key': API_KEY,
            'start_date': iso_date(start_date),
            'end_date': iso_date(end_date),
            'page': page
        }
        try:
            resp = requests.get(url, params=params)
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
            print(f"HTTPError on changes {endpoint} page {page}: {he} (status {status})")
            if status == 429:
                print("Rate limited, sleeping 2s then retrying...")
                time.sleep(2)
                continue
            break
        except Exception as e:
            print(f"Error calling changes {endpoint} page {page}: {e}")
            break
    return ids

def fetch_movie_detail_and_credits(movie_id):
    try:
        detail = requests.get(f"{TMDB_BASE}/movie/{movie_id}", params={'api_key':API_KEY}).json()
        credits = requests.get(f"{TMDB_BASE}/movie/{movie_id}/credits", params={'api_key':API_KEY}).json()
        return detail, credits
    except Exception as e:
        print(f"Movie fetch error {movie_id}: {e}")
        return None, None

def fetch_tv_detail_and_aggregate(tv_id):
    try:
        detail = requests.get(f"{TMDB_BASE}/tv/{tv_id}", params={'api_key':API_KEY}).json()
        agg = requests.get(f"{TMDB_BASE}/tv/{tv_id}/aggregate_credits", params={'api_key':API_KEY, 'language':'en-US'}).json()
        return detail, agg
    except Exception as e:
        print(f"TV fetch error {tv_id}: {e}")
        return None, None

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

    con.execute("""
        CREATE TABLE IF NOT EXISTS last_updates (
            job_name VARCHAR,
            last_run TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Migration: drop primary key from last_updates to allow append-only inserts
    try:
        con.execute("ALTER TABLE last_updates DROP CONSTRAINT last_updates_pkey;")
    except Exception:
        pass  # Constraint already dropped or doesn't exist — nothing to do

    # Add inserted_at and updated_at columns to existing tables (idempotent)
    for table in ['movies', 'movie_cast', 'movie_crew', 'tv_shows', 'tv_show_cast_crew']:
        con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
        con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

# --- upsert helpers ---
def upsert_table_from_rows(con, rows, table_name, canonical_cols, key_col=None, dry_run=False):
    """
    Upsert rows (list of dict) into table_nam
    e using canonical_cols ordering.
    Preserves inserted_at from existing rows, sets updated_at to current timestamp.
    If dry_run is True, print what would be done, skip DB writes, and write a summary CSV preview.
    """
    if not rows:
        return

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
        print(f"[DRY RUN] Would upsert {len(df_reindexed)} rows into {table_name}.")
        # Write summary CSV preview
        import csv
        import tempfile
        from datetime import datetime
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
        print(f"[DRY RUN] Preview CSV written: {preview_path}")
        return

    con.register('tmp_df', df_reindexed)
    try:
        # Preserve old inserted_at values before delete
        if key:
            # Qualify key column for all SQL statements
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE old_inserted_at AS
                SELECT o.{key}, o.inserted_at FROM {table_name} o
                WHERE o.{key} IN (SELECT DISTINCT t.{key} FROM tmp_df t);
            """)
            con.execute(f"DELETE FROM {table_name} WHERE {key} IN (SELECT DISTINCT t.{key} FROM tmp_df t);")

        # Insert with preserved inserted_at (COALESCE to keep old value) and new updated_at
        insert_cols = ", ".join(canonical_cols)
        if key:
            # Qualify ambiguous columns in SELECT
            qualified_insert_cols = ", ".join([f"t.{col}" for col in canonical_cols])
            con.execute(f"""
                INSERT INTO {table_name} ({insert_cols}, inserted_at, updated_at)
                SELECT {qualified_insert_cols},
                       COALESCE(o.inserted_at, CURRENT_TIMESTAMP) AS inserted_at,
                       CURRENT_TIMESTAMP AS updated_at
                FROM tmp_df t
                LEFT JOIN old_inserted_at o ON t.{key} = o.{key};
            """)
        else:
            con.execute(f"INSERT INTO {table_name} ({insert_cols}, inserted_at, updated_at) SELECT {insert_cols}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM tmp_df;")
    finally:
        try:
            con.unregister('tmp_df')
        except Exception:
            pass
        try:
            con.execute("DROP TABLE IF EXISTS old_inserted_at;")
        except Exception:
            pass

def _format_seconds(s: float) -> str:
    hrs, rem = divmod(s, 3600)
    mins, secs = divmod(rem, 60)
    return f"{int(hrs)}h {int(mins)}m {secs:.2f}s"

def run(sample_only=0, force_days=None, dry_run=False):
    run_start = time.time()

    # Connect to MotherDuck
    con = get_connection()
    print("✓ Connected to MotherDuck")

    # Ensure tables exist
    ensure_tables(con)

    if dry_run:
        print("\n--- DRY RUN: No database writes will be performed ---")
    else:
        print("\n--- Live run: TMDB_backup mirror created by backup_to_glacier.py before this step ---")


    # Determine last_run
    if force_days is not None:
        last_run = datetime.now(timezone.utc) - timedelta(days=force_days)
        print(f"Forcing update: fetching changes from last {force_days} days")
    else:
        last_run = get_last_run(con)
    now = datetime.now(timezone.utc)
    print(f"Last run: {last_run.isoformat()}, now: {now.isoformat()}")

    movie_ids = call_changes('movie', last_run, now)
    tv_ids = call_changes('tv', last_run, now)

    if sample_only and sample_only > 0:
        movie_ids = movie_ids[:sample_only]
        tv_ids = tv_ids[:sample_only]

    total_changes = len(movie_ids) + len(tv_ids)
    print(f"Found {len(movie_ids)} changed movies, {len(tv_ids)} changed tv shows (total changes: {total_changes})")

    # Fetch details phase
    fetch_start = time.time()
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
            print(
                f"  Movies: {pct}% ({i}/{len(movie_ids)} of {len(movie_ids)} total to upsert) | "
                f"elapsed: {_format_seconds(elapsed)} | "
                f"remaining: ~{_format_seconds(remaining)} | "
                f"ETA: {(datetime.now() + timedelta(seconds=remaining)).strftime('%H:%M:%S UTC')}",
                flush=True
            )
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

    tv_rows, tv_cast_rows = [], []
    processed_tv_count = 0
    milestones = {int(len(tv_ids) * p) for p in [0.25, 0.50, 0.75, 1.0]}
    tv_fetch_start = time.time()
    for i, tid in enumerate(tv_ids):
        if i in milestones and i > 0:
            pct = round(i / len(tv_ids) * 100)
            elapsed = time.time() - tv_fetch_start
            forecast_total = (elapsed / i) * len(tv_ids)
            remaining = forecast_total - elapsed
            print(
                f"  TV shows: {pct}% ({i}/{len(tv_ids)} of {len(tv_ids)} total to upsert) | "
                f"elapsed: {_format_seconds(elapsed)} | "
                f"remaining: ~{_format_seconds(remaining)} | "
                f"ETA: {(datetime.now() + timedelta(seconds=remaining)).strftime('%H:%M:%S UTC')}",
                flush=True
            )
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

    fetch_end = time.time()
    fetch_elapsed = fetch_end - fetch_start
    processed_count = processed_movie_count + processed_tv_count
    avg_per_item = fetch_elapsed / processed_count if processed_count > 0 else 0
    predicted_fetch_total = avg_per_item * total_changes

    print(f"Fetch phase: processed {processed_count} items in {_format_seconds(fetch_elapsed)} (avg {_format_seconds(avg_per_item)} per item)")
    if sample_only and sample_only > 0 and processed_count < total_changes:
        print(f"Prediction: estimated full fetch time for {total_changes} items is {_format_seconds(predicted_fetch_total)} based on sample")

    # Upsert phase
    upsert_start = time.time()
    
    upsert_table_from_rows(con, movies_rows, 'movies', MOVIES_COLS, key_col='id', dry_run=dry_run)
    upsert_table_from_rows(con, movie_cast_rows, 'movie_cast', MOVIE_CAST_COLS, key_col='movie_id', dry_run=dry_run)
    upsert_table_from_rows(con, movie_crew_rows, 'movie_crew', MOVIE_CREW_COLS, key_col='movie_id', dry_run=dry_run)
    upsert_table_from_rows(con, tv_rows, 'tv_shows', TV_SHOWS_COLS, key_col='id', dry_run=dry_run)
    upsert_table_from_rows(con, tv_cast_rows, 'tv_show_cast_crew', TV_CAST_COLS, key_col='tv_id', dry_run=dry_run)
    if not dry_run:
        set_last_run(con, now)
    else:
        print("[DRY RUN] Would update last_run in DB.")

    upsert_end = time.time()
    upsert_elapsed = upsert_end - upsert_start

    run_end = time.time()
    total_elapsed = run_end - run_start

    print(f"DB upsert phase took {_format_seconds(upsert_elapsed)}")
    print(f"Total run time: {_format_seconds(total_elapsed)}")

    if sample_only and sample_only > 0 and processed_count > 0:
        estimated_total_run = predicted_fetch_total + upsert_elapsed
        print(f"Estimated total run time for full dataset ({total_changes} items): {_format_seconds(estimated_total_run)} (based on sample)")

    con.close()
    print("\n✓ Update complete!")

# Configure logging
logging.basicConfig(level=logging.INFO)

def backup_to_glacier():
    logging.info("Starting backup to Glacier...")
    try:
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backup_to_glacier.py")
        subprocess.run(
            ["python", script_path],
            check=True,
        )
        logging.info("Backup to Glacier completed successfully.")
    except subprocess.TimeoutExpired:
        logging.error("Backup timed out after 30 minutes — possibly hit MotherDuck compute limit.")
        return False
    except subprocess.CalledProcessError as e:
        logging.error("Backup to Glacier failed: %s", e)
        return False
    return True

def run_update_with_backup(sample_only=0, force_days=None, dry_run=False):
    # Backup runs as a separate Thursday workflow — skipped here
    logging.info("Backup handled by separate scheduled workflow. Proceeding with update...")
    run(sample_only=sample_only, force_days=force_days, dry_run=dry_run)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TMDB weekly update job')
    parser.add_argument('--sample', type=int, default=0, help='Process only N changed ids (movies and tv) for quick testing')
    parser.add_argument('--force', type=int, default=None, metavar='DAYS',
                        help='Ignore stored last_run and fetch changes from DAYS ago (e.g., --force 30 for last 30 days, --force 7 for last week)')
    parser.add_argument('--dry-run', action='store_true', help='Run without writing to the database (for manual testing)')
    args = parser.parse_args()

    run_update_with_backup(sample_only=args.sample, force_days=args.force, dry_run=args.dry_run)
