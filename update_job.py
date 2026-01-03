import os
import time
import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import math
import argparse

API_KEY = os.getenv('TMDBAPIKEY')
DB_PATH = 'TMDB'
TMDB_BASE = 'https://api.themoviedb.org/3'

# --- canonical column lists ---
MOVIES_COLS = [
    'id','title','release_date','original_language','popularity','vote_count',
    'adult','backdrop_path','belongs_to_collection','budget','genres','homepage',
    'imdb_id','origin_country','original_title','overview','poster_path',
    'production_companies','production_countries','revenue','runtime',
    'spoken_languages','status','tagline','video','vote_average'
]

MOVIE_CAST_COLS = [
    'movie_id','person_id','name','credit_id','character','order','gender',
    'profile_path','known_for_department','popularity','original_name','cast_id'
]

MOVIE_CREW_COLS = [
    'movie_id','person_id','name','credit_id','gender','profile_path',
    'known_for_department','popularity','original_name','adult','department','job'
]

TV_SHOWS_COLS = [
    'id','episode_run_time','homepage','in_production','last_air_date',
    'number_of_episodes','number_of_seasons','origin_country',
    'production_countries','status','type'
]

TV_CAST_COLS = [
    'tv_id','person_id','name','credit_id','character','order','gender',
    'profile_path','known_for_department','popularity','original_name',
    'roles','total_episode_count','cast_id','also_known_as'
]

# --- helpers ---
def iso_date(dt): return dt.strftime('%Y-%m-%d')

def get_last_run(con, job_name='weekly_update'):
    con.execute('CREATE TABLE IF NOT EXISTS last_updates (job_name VARCHAR PRIMARY KEY, last_run TIMESTAMP);')
    row = con.execute("SELECT last_run FROM last_updates WHERE job_name = ?;", [job_name]).fetchone()
    if row and row[0] is not None:
        try:
            return datetime.fromisoformat(row[0])
        except Exception:
            try:
                return datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                pass
    return datetime.now(datetime.timezone.utc) - timedelta(days=7)

def set_last_run(con, ts, job_name='weekly_update'):
    con.execute("INSERT OR REPLACE INTO last_updates (job_name, last_run) VALUES (?, ?);", [job_name, ts.isoformat()])

def call_changes(endpoint, start_date, end_date):
    url = f"{TMDB_BASE}/{endpoint}/changes"
    params = {'api_key': API_KEY, 'start_date': iso_date(start_date), 'end_date': iso_date(end_date)}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return [r['id'] for r in data.get('results', [])]
    except Exception as e:
        print(f"Error calling changes {endpoint}: {e}")
        return []

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
            budget INTEGER,
            genres VARCHAR,
            homepage VARCHAR,
            imdb_id VARCHAR,
            origin_country VARCHAR,
            original_title VARCHAR,
            overview VARCHAR,
            poster_path VARCHAR,
            production_companies VARCHAR,
            production_countries VARCHAR,
            revenue INTEGER,
            runtime INTEGER,
            spoken_languages VARCHAR,
            status VARCHAR,
            tagline VARCHAR,
            video BOOLEAN,
            vote_average DOUBLE
        );
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS movie_cast (
            movie_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            character VARCHAR,
            "order" INTEGER,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            cast_id BIGINT
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
            job VARCHAR
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
            type VARCHAR
        );
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS tv_show_cast_crew (
            tv_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            character VARCHAR,
            "order" INTEGER,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            roles VARCHAR,
            total_episode_count INTEGER,
            cast_id BIGINT,
            also_known_as VARCHAR
        );
    """)

# --- upsert helpers (reindex to canonical cols so all columns are considered) ---
def upsert_table_from_rows(con, rows, table_name, canonical_cols, key_col=None):
    if not rows:
        return
    df = pd.DataFrame(rows)
    # serialize lists/dicts to string to avoid nested type problems
    for c in df.columns:
        df[c] = df[c].apply(lambda v: (str(v) if isinstance(v, (list, dict)) else v))
    # ensure all canonical columns exist (order consistent for deletes)
    df = df.reindex(columns=[c for c in canonical_cols if c in df.columns] + [c for c in canonical_cols if c not in df.columns], fill_value=None)
    # register and upsert
    con.register('tmp_df', df)
    # delete existing rows by key if provided, else delete by distinct id column if present
    if key_col and key_col in df.columns:
        con.execute(f"DELETE FROM {table_name} WHERE {key_col} IN (SELECT DISTINCT {key_col} FROM tmp_df);")
    else:
        # try to detect id-like column for deletion
        if any(k in df.columns for k in ('id','movie_id','tv_id')):
            key = 'id' if 'id' in df.columns else ('movie_id' if 'movie_id' in df.columns else 'tv_id')
            con.execute(f"DELETE FROM {table_name} WHERE {key} IN (SELECT DISTINCT {key} FROM tmp_df);")
    # build insert column list from df columns (tmp_df columns order)
    insert_cols = ", ".join(df.columns)
    con.execute(f"INSERT INTO {table_name} ({insert_cols}) SELECT {insert_cols} FROM tmp_df;")
    con.unregister('tmp_df')

# --- main run ---
def run(dry_run=False, in_memory=False, sample_only=0):
    # choose DB: in-memory or real
    db_to_use = ':memory:' if (dry_run or in_memory) else DB_PATH
    con = duckdb.connect(database=db_to_use, read_only=False)

    # ensure tables exist when testing in-memory
    ensure_tables(con)

    last_run = get_last_run(con) if not dry_run else (datetime.utcnow() - timedelta(days=7))
    now = datetime.utcnow()
    print(f"Last run: {last_run.isoformat()}, now: {now.isoformat()} (dry_run={dry_run}, in_memory={in_memory})")

    movie_ids = call_changes('movie', last_run, now)
    tv_ids = call_changes('tv', last_run, now)

    # apply sampling for quick tests
    if sample_only and sample_only > 0:
        movie_ids = movie_ids[:sample_only]
        tv_ids = tv_ids[:sample_only]

    print(f"Found {len(movie_ids)} changed movies, {len(tv_ids)} changed tv shows (sample_only={sample_only})")

    movies_rows = []
    movie_cast_rows = []
    movie_crew_rows = []
    for mid in movie_ids:
        detail, credits = fetch_movie_detail_and_credits(mid)
        if not detail:
            continue
        #Some databases can't process nested structures (lists or dictionaries), so flatten them to strings
        detail_row = {k: (str(v) if isinstance(v, (list, dict)) else v) for k,v in detail.items()}
        # ensure canonical id present
        detail_row.setdefault('id', mid)
        movies_rows.append({k: detail_row.get(k) for k in MOVIES_COLS if k in detail_row or k in MOVIES_COLS})

        for c in credits.get('cast', []):
            movie_cast_rows.append({
                'movie_id': mid,
                'person_id': c.get('id'),
                'name': c.get('name'),
                'credit_id': c.get('credit_id'),
                'character': c.get('character'),
                'order': c.get('order'),
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
        time.sleep(0.12)

    tv_rows = []
    tv_cast_rows = []
    for tid in tv_ids:
        detail, agg = fetch_tv_detail_and_aggregate(tid)
        if not detail:
            continue
        detail_row = {k: (str(v) if isinstance(v, (list, dict)) else v) for k,v in detail.items()}
        detail_row.setdefault('id', tid)
        tv_rows.append({k: detail_row.get(k) for k in TV_SHOWS_COLS if k in detail_row or k in TV_SHOWS_COLS})
        for c in agg.get('cast', []):
            tv_cast_rows.append({
                'tv_id': tid,
                'person_id': c.get('id'),
                'name': c.get('name'),
                'credit_id': c.get('credit_id'),
                'character': c.get('character'),
                'order': c.get('order'),
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
        time.sleep(0.12)

    # At upsert time: either preview/save or write to DB
    if dry_run:
        # print/count previews and save small CSVs for inspection
        print("DRY RUN: would upsert the following counts:")
        print(f"  movies: {len(movies_rows)}")
        print(f"  movie_cast: {len(movie_cast_rows)}")
        print(f"  movie_crew: {len(movie_crew_rows)}")
        print(f"  tv_shows: {len(tv_rows)}")
        print(f"  tv_cast: {len(tv_cast_rows)}")

        timestamp = now.strftime('%Y%m%dT%H%M%S')
        if movies_rows:
            pd.DataFrame(movies_rows).head(50).to_csv(f'/tmp/update_job_movies_preview_{timestamp}.csv', index=False)
            print(f"  Saved movies preview to /tmp/update_job_movies_preview_{timestamp}.csv")
        if movie_cast_rows:
            pd.DataFrame(movie_cast_rows).head(200).to_csv(f'/tmp/update_job_movie_cast_preview_{timestamp}.csv', index=False)
            print(f"  Saved movie_cast preview to /tmp/update_job_movie_cast_preview_{timestamp}.csv")
        if movie_crew_rows:
            pd.DataFrame(movie_crew_rows).head(200).to_csv(f'/tmp/update_job_movie_crew_preview_{timestamp}.csv', index=False)
            print(f"  Saved movie_crew preview to /tmp/update_job_movie_crew_preview_{timestamp}.csv")
        if tv_rows:
            pd.DataFrame(tv_rows).head(50).to_csv(f'/tmp/update_job_tv_preview_{timestamp}.csv', index=False)
            print(f"  Saved tv_shows preview to /tmp/update_job_tv_preview_{timestamp}.csv")
        if tv_cast_rows:
            pd.DataFrame(tv_cast_rows).head(200).to_csv(f'/tmp/update_job_tv_cast_preview_{timestamp}.csv', index=False)
            print(f"  Saved tv_cast preview to /tmp/update_job_tv_cast_preview_{timestamp}.csv")
        print("DRY RUN complete. No DB changes made.")
    else:
        # real upsert + record last run
        upsert_table_from_rows(con, movies_rows, 'movies', MOVIES_COLS, key_col='id')
        upsert_table_from_rows(con, movie_cast_rows, 'movie_cast', MOVIE_CAST_COLS, key_col='movie_id')
        upsert_table_from_rows(con, movie_crew_rows, 'movie_crew', MOVIE_CREW_COLS, key_col='movie_id')
        upsert_table_from_rows(con, tv_rows, 'tv_shows', TV_SHOWS_COLS, key_col='id')
        upsert_table_from_rows(con, tv_cast_rows, 'tv_show_cast_crew', TV_CAST_COLS, key_col='tv_id')
        set_last_run(con, now)
        print("Live update complete.")

    con.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TMDB weekly update job')
    parser.add_argument('--dry-run', action='store_true', help='Do not write to production DB; save preview CSVs to /tmp')
    parser.add_argument('--in-memory', action='store_true', help='Run against an in-memory DuckDB (no disk writes)')
    parser.add_argument('--sample', type=int, default=0, help='Process only N changed ids (movies and tv) for quick testing')
    args = parser.parse_args()

    run(dry_run=args.dry_run, in_memory=args.in_memory, sample_only=args.sample)