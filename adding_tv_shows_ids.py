import tmdbsimple as tmdb
import os
import duckdb
import time
import requests
from datetime import datetime, timedelta
import calendar

# Set your TMDB API key from environment variables
# Make sure you have TMDBAPIKEY set in your environment
tmdb.API_KEY = os.getenv('TMDBAPIKEY')

# Connect to the DuckDB database
con = duckdb.connect(database='TMDB', read_only=False)

# Create the tv_shows table if it doesn't exist
# This table will primarily store the IDs, and other scripts can enrich the data
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

def populate_all_tv_show_ids(start_year=1936, end_year=None):
    """
    Populates the 'tv_shows' table with TV show IDs by iterating through
    release dates (month by month, year by year) using the TMDB Discover API.

    Args:
        start_year (int): The earliest year to start fetching TV shows from.
        end_year (int): The latest year to fetch TV shows up to. If None,
                        defaults to the current year.
    """
    if end_year is None:
        end_year = datetime.now().year

    discover = tmdb.Discover()
    inserted_count = 0
    total_api_calls = 0
    start_time = time.time()

    # Fetch all existing TV show IDs from the database to skip duplicates
    existing_ids = set(row[0] for row in con.execute("SELECT id FROM tv_shows").fetchall())

    print(f"Starting to populate all TV show IDs from TMDB from {start_year} to {end_year}...\n")

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # Determine the first and last day of the month
            _, num_days = calendar.monthrange(year, month)
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-{num_days:02d}"

            current_page = 1
            total_pages_for_month = 1

            print(f"Fetching TV shows for {year}-{month:02d} (from {start_date} to {end_date})...")

            while current_page <= total_pages_for_month:
                try:
                    response = discover.tv(
                        page=current_page,
                        first_air_date_gte=start_date,
                        first_air_date_lte=end_date,
                        sort_by='first_air_date.asc' # Sort by air date to ensure consistent pagination
                    )
                    total_api_calls += 1

                    if not response['results']:
                        print(f"No more results found for {year}-{month:02d} on page {current_page}.")
                        break

                    total_pages_for_month = response['total_pages']
                    if total_pages_for_month > 500:
                        total_pages_for_month = 500
                        print(f"WARNING: More than 500 pages for {year}-{month:02d}. Only fetching first 500 pages.")

                    for show in response['results']:
                        tv_show_id = show['id']
                        if tv_show_id in existing_ids:
                            print(f"Skipping already existing ID: {tv_show_id}")
                            continue
                        tv_show_name = show['name'].replace("'", "''")
                        
                        print(f"Attempting to insert ID: {tv_show_id}, Name: {tv_show_name}")
                        try:
                            # Use INSERT OR IGNORE to avoid inserting duplicate IDs
                            con.execute(f"INSERT OR IGNORE INTO tv_shows (id, name) VALUES ({tv_show_id}, '{tv_show_name}');")
                            inserted_count += 1
                            existing_ids.add(tv_show_id)
                            print(f"Successfully processed ID: {tv_show_id}")
                        except Exception as e:
                            print(f"ERROR: Could not insert TV show ID {tv_show_id} into database: {e}")
                            continue
                    
                    current_page += 1

                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    minutes = int(elapsed_time // 60)
                    remaining_seconds = elapsed_time % 60
                    print(f"  Processed page {current_page-1}/{total_pages_for_month} for {year}-{month:02d}. Total unique IDs inserted: {inserted_count}. Total API calls: {total_api_calls}. Elapsed time: {minutes}m{remaining_seconds:.2f}s")
                    
                    time.sleep(0.25) 

                except requests.exceptions.HTTPError as e:
                    print(f"ERROR: HTTPError while fetching {year}-{month:02d}, page {current_page}: {e}")
                    if e.response.status_code == 429: # Too Many Requests
                        print("Rate limit hit. Waiting for 20 seconds before retrying this page...")
                        time.sleep(20)
                    else:
                        print(f"Skipping page {current_page} for {year}-{month:02d} due to HTTP error.")
                        current_page += 1
                    continue
                except Exception as e:
                    print(f"ERROR: An unexpected error occurred while fetching {year}-{month:02d}, page {current_page}: {e}")
                    print(f"Skipping page {current_page} for {year}-{month:02d} due to unexpected error.")
                    current_page += 1
                    continue

    print(f"\nTV show ID population complete.")
    print(f"Total unique TV show IDs inserted: {inserted_count}.")
    print(f"Total API calls made: {total_api_calls}.")

# You can adjust start_year and end_year as needed.
populate_all_tv_show_ids(start_year=1936, end_year=None)

# Close the DuckDB connection
con.close()
