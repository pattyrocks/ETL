import os
import time
import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import math

API_KEY = os.getenv('TMDBAPIKEY')
DB_PATH = 'TMDB'
TMDB_BASE = 'https://api.themoviedb.org/3'

def iso_date(dt):
    return dt.strftime('%Y-%m-%d')

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

def upsert_movies_and_cast_and_crew(con, movies_rows, cast_rows, crew_rows):
    if movies_rows:
        df_movies = pd.DataFrame(movies_rows)
        con.register('df_movies', df_movies)
        con.execute("CREATE TABLE IF NOT EXISTS movies AS SELECT * FROM df_movies WHERE 1=0;")
        con.execute("DELETE FROM movies WHERE id IN (SELECT id FROM df_movies);")
        con.execute("INSERT INTO movies SELECT * FROM df_movies;")
        con.unregister('df_movies')
    if cast_rows:
        df_cast = pd.DataFrame(cast_rows)
        con.register('df_cast', df_cast)
        con.execute("CREATE TABLE IF NOT EXISTS movie_cast AS SELECT * FROM df_cast WHERE 1=0;")
        con.execute("DELETE FROM movie_cast WHERE movie_id IN (SELECT DISTINCT movie_id FROM df_cast);")
        con.execute("INSERT INTO movie_cast SELECT * FROM df_cast;")
        con.unregister('df_cast')
    if crew_rows:
        df_crew = pd.DataFrame(crew_rows)
        con.register('df_crew', df_crew)
        con.execute("CREATE TABLE IF NOT EXISTS movie_crew AS SELECT * FROM df_crew WHERE 1=0;")
        con.execute("DELETE FROM movie_crew WHERE movie_id IN (SELECT DISTINCT movie_id FROM df_crew);")
        con.execute("INSERT INTO movie_crew SELECT * FROM df_crew;")
        con.unregister('df_crew')

def upsert_tv_and_cast(con, tv_rows, tv_cast_rows):
    if tv_rows:
        df_tv = pd.DataFrame(tv_rows)
        con.register('df_tv', df_tv)
        con.execute("CREATE TABLE IF NOT EXISTS tv_shows AS SELECT * FROM df_tv WHERE 1=0;")
        con.execute("DELETE FROM tv_shows WHERE id IN (SELECT id FROM df_tv);")
        con.execute("INSERT INTO tv_shows SELECT * FROM df_tv;")
        con.unregister('df_tv')
    if tv_cast_rows:
        df = pd.DataFrame(tv_cast_rows)
        con.register('df_tv_cast', df)
        con.execute("CREATE TABLE IF NOT EXISTS tv_show_cast_crew AS SELECT * FROM df_tv_cast WHERE 1=0;")
        con.execute("DELETE FROM tv_show_cast_crew WHERE tv_id IN (SELECT DISTINCT tv_id FROM df_tv_cast);")
        con.execute("INSERT INTO tv_show_cast_crew SELECT * FROM df_tv_cast;")
        con.unregister('df_tv_cast')

def run():
    con = duckdb.connect(database=DB_PATH, read_only=False)
    last_run = get_last_run(con)
    now = datetime.now(datetime.timezone.utc)
    print(f"Last run: {last_run.isoformat()}, now: {now.isoformat()}")

    movie_ids = call_changes('movie', last_run, now)
    tv_ids = call_changes('tv', last_run, now)

    print(f"Found {len(movie_ids)} changed movies, {len(tv_ids)} changed tv shows.")

    movies_rows = []
    movie_cast_rows = []
    movie_crew_rows = []
    for mid in movie_ids:
        detail, credits = fetch_movie_detail_and_credits(mid)
        if not detail:
            continue
        #Some databases can't process nested structures (lists or dictionaries), so flatten them to strings
        detail_row = {k: (str(v) if isinstance(v, (list, dict)) else v) for k,v in detail.items()}
        movies_rows.append(detail_row)

        for c in credits.get('cast', []):
            movie_cast_rows.append({
                'movie_id': mid,
                'person_id': c.get('id'),
                'name': c.get('name'),
                'character': c.get('character'),
                'credit_id': c.get('credit_id'),
                'order': c.get('order'),
                'profile_path': c.get('profile_path'),
                'gender': c.get('gender')
            })
        for crew in credits.get('crew', []):
            movie_crew_rows.append({
                'movie_id': mid,
                'person_id': crew.get('id'),
                'name': crew.get('name'),
                'job': crew.get('job'),
                'department': crew.get('department'),
                'credit_id': crew.get('credit_id'),
                'profile_path': crew.get('profile_path')
            })
        time.sleep(0.15)

    tv_rows = []
    tv_cast_rows = []
    for tid in tv_ids:
        detail, agg = fetch_tv_detail_and_aggregate(tid)
        if not detail:
            continue
        tv_row = {k: (str(v) if isinstance(v, (list, dict)) else v) for k,v in detail.items()}
        tv_rows.append(tv_row)
        for c in agg.get('cast', []):
            tv_cast_rows.append({
                'tv_id': tid,
                'person_id': c.get('id'),
                'name': c.get('name'),
                'roles': str(c.get('roles')),
                'total_episode_count': c.get('total_episode_count'),
                'profile_path': c.get('profile_path')
            })
        time.sleep(0.15)

    upsert_movies_and_cast_and_crew(con, movies_rows, movie_cast_rows, movie_crew_rows)
    upsert_tv_and_cast(con, tv_rows, tv_cast_rows)

    set_last_run(con, now)
    con.close()
    print("Update finished.")

if __name__ == '__main__':
    run()