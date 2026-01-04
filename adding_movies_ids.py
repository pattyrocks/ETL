import tmdbsimple as tmdb
import os
import duckdb
import time
import requests
from datetime import datetime
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed

tmdb.API_KEY = os.getenv('TMDBAPIKEY')

con = duckdb.connect(database='TMDB', read_only=False)

con.execute('''
    CREATE TABLE IF NOT EXISTS movies (
        id BIGINT PRIMARY KEY,
        title VARCHAR,
        release_date VARCHAR,
        original_language VARCHAR,
        popularity DOUBLE,
        vote_count INTEGER
    );
''')

def fetch_movie_page(year, month, page, start_date, end_date):
    discover = tmdb.Discover()
    try:
        response = discover.movie(
            page=page,
            release_date_gte=start_date,
            release_date_lte=end_date,
            sort_by='release_date.asc'
        )
        return response['results']
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"Rate limit hit on {year}-{month:02d} page {page}. Waiting 20s.")
            time.sleep(20)
            return fetch_movie_page(year, month, page, start_date, end_date)
        else:
            print(f"HTTPError on {year}-{month:02d} page {page}: {e}")
            return []
    except Exception as e:
        print(f"Error on {year}-{month:02d} page {page}: {e}")
        return []

def get_existing_movie_ids(con):
    result = con.execute("SELECT id FROM movies").fetchall()
    return set(row[0] for row in result)

def populate_all_movie_ids_parallel(start_year=1874, end_year=datetime.now().year, max_workers=8):

    inserted_count = 0
    total_api_calls = 0
    start_time = time.time()

    # Fetch existing IDs once at the start
    existing_ids = get_existing_movie_ids(con)

    print(f"Starting to populate all Movie IDs from TMDB from {start_year} to {end_year}...\n")

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            _, num_days = calendar.monthrange(year, month)
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-{num_days:02d}"

            print(f"Fetching Movies for {year}-{month:02d} (from {start_date} to {end_date})...")

            discover = tmdb.Discover()
            try:
                first_response = discover.movie(
                    page=1,
                    release_date_gte=start_date,
                    release_date_lte=end_date,
                    sort_by='release_date.asc'
                )
                total_api_calls += 1
                total_pages = first_response.get('total_pages', 1)
                if total_pages > 500:
                    total_pages = 500
                    print(f"WARNING: More than 500 pages for {year}-{month:02d}. Only fetching first 500 pages.")
                all_results = [first_response['results']]
            except Exception as e:
                print(f"Failed to fetch first page for {year}-{month:02d}: {e}")
                continue

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for page in range(2, total_pages + 1):
                    futures.append(executor.submit(
                        fetch_movie_page, year, month, page, start_date, end_date
                    ))

                for future in as_completed(futures):
                    results = future.result()
                    all_results.append(results)
                    total_api_calls += 1

            # insert into DB, skipping already existing IDs
            for page_results in all_results:
                for movie in page_results:
                    movie_id = movie['id']
                    if movie_id in existing_ids:
                        continue 
                    title = movie['title'].replace("'", "''")
                    release_date = movie.get('release_date', '')
                    original_language = movie.get('original_language', '')
                    popularity = movie.get('popularity', 0.0)
                    vote_count = movie.get('vote_count', 0)
                    try:
                        con.execute(
                            f"INSERT OR IGNORE INTO movies (id, title, release_date, original_language, popularity, vote_count) "
                            f"VALUES ({movie_id}, '{title}', '{release_date}', '{original_language}', {popularity}, {vote_count});"
                        )
                        inserted_count += 1
                        existing_ids.add(movie_id)
                    except Exception as e:
                        print(f"ERROR: Could not insert Movie ID {movie_id} into database: {e}")
                        continue

            elapsed_time = time.time() - start_time
            minutes = int(elapsed_time // 60)
            remaining_seconds = elapsed_time % 60
            print(f"  Finished {year}-{month:02d}. Total unique IDs inserted: {inserted_count}. Total API calls: {total_api_calls}. Elapsed time: {minutes}m{remaining_seconds:.2f}s")

    print(f"\nMovie ID population complete.")
    print(f"Total unique Movie IDs inserted: {inserted_count}.")
    print(f"Total API calls made: {total_api_calls}.")

populate_all_movie_ids_parallel(start_year=1874, end_year=datetime.now().year)

con.close()