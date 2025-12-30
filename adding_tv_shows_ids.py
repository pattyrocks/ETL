import tmdbsimple as tmdb
import os
import duckdb
import time
import requests
from datetime import datetime, timedelta
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set your TMDB API key from environment variables
tmdb.API_KEY = os.getenv('TMDBAPIKEY')

# Connect to the DuckDB database
con = duckdb.connect(database='TMDB', read_only=False)

# Create the tv_shows table if it doesn't exist
con.execute('''
    CREATE TABLE IF NOT EXISTS tv_shows (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        episode_run_time VARCHAR[],
        homepage VARCHAR,
        in_production BOOLEAN,
        last_air_date VARCHAR,
        number_of_episodes INTEGER,
        number_of_seasons INTEGER,
        origin_country VARCHAR[],
        production_countries VARCHAR,
        status VARCHAR,
        type VARCHAR
    );
''')

def fetch_tv_page(year, month, page, start_date, end_date):
    discover = tmdb.Discover()
    try:
        response = discover.tv(
            page=page,
            first_air_date_gte=start_date,
            first_air_date_lte=end_date,
            sort_by='first_air_date.asc'
        )
        return response['results']
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"Rate limit hit on {year}-{month:02d} page {page}. Waiting 20s.")
            time.sleep(20)
            return fetch_tv_page(year, month, page, start_date, end_date)
        else:
            print(f"HTTPError on {year}-{month:02d} page {page}: {e}")
            return []
    except Exception as e:
        print(f"Error on {year}-{month:02d} page {page}: {e}")
        return []

def get_existing_tv_show_ids(con):
    result = con.execute("SELECT id FROM tv_shows").fetchall()
    return set(row[0] for row in result)

def populate_all_tv_show_ids_parallel(start_year=1827, end_year=None, max_workers=8):
    if end_year is None:
        end_year = datetime.now().year

    inserted_count = 0
    total_api_calls = 0
    start_time = time.time()

    # Fetch all existing TV show IDs from the database to skip duplicates
    existing_ids = get_existing_tv_show_ids(con)

    print(f"Starting to populate all TV show IDs from TMDB from {start_year} to {end_year}...\n")

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            _, num_days = calendar.monthrange(year, month)
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-{num_days:02d}"

            print(f"Fetching TV shows for {year}-{month:02d} (from {start_date} to {end_date})...")

            discover = tmdb.Discover()
            try:
                first_response = discover.tv(
                    page=1,
                    first_air_date_gte=start_date,
                    first_air_date_lte=end_date,
                    sort_by='first_air_date.asc'
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
                        fetch_tv_page, year, month, page, start_date, end_date
                    ))

                for future in as_completed(futures):
                    results = future.result()
                    all_results.append(results)
                    total_api_calls += 1

            # insert into DB, skipping already existing IDs
            for page_results in all_results:
                for show in page_results:
                    tv_show_id = show['id']
                    if tv_show_id in existing_ids:
                        continue
                    tv_show_name = show['name'].replace("'", "''")
                    try:
                        con.execute(
                            f"INSERT OR IGNORE INTO tv_shows (id, name) VALUES ({tv_show_id}, '{tv_show_name}');"
                        )
                        inserted_count += 1
                        existing_ids.add(tv_show_id)
                    except Exception as e:
                        print(f"ERROR: Could not insert TV show ID {tv_show_id} into database: {e}")
                        continue

            elapsed_time = time.time() - start_time
            minutes = int(elapsed_time // 60)
            remaining_seconds = elapsed_time % 60
            print(f"  Finished {year}-{month:02d}. Total unique IDs inserted: {inserted_count}. Total API calls: {total_api_calls}. Elapsed time: {minutes}m{remaining_seconds:.2f}s")

    print(f"\nTV show ID population complete.")
    print(f"Total unique TV show IDs inserted: {inserted_count}.")
    print(f"Total API calls made: {total_api_calls}.")

# You can adjust start_year and end_year as needed.
populate_all_tv_show_ids_parallel()

# Close the DuckDB connection
con.close()
