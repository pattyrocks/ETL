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
# This will create 'tmdb.duckdb' if it doesn't exist
con = duckdb.connect(database='tmdb.duckdb', read_only=False)

# Create the tv_shows table if it doesn't exist
# This table will primarily store the IDs, and other scripts can enrich the data
con.execute('''
    CREATE TABLE IF NOT EXISTS tv_shows (
        id INTEGER PRIMARY KEY,
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

def populate_all_tv_show_ids_by_date(start_year=1936, end_year=None):
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

    print(f"Starting to populate all TV show IDs from TMDB from {start_year} to {end_year}...\n")

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # Determine the first and last day of the month
            _, num_days = calendar.monthrange(year, month)
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-{num_days:02d}"

            current_page = 1
            total_pages_for_month = 1 # Initialize to enter the loop

            print(f"Fetching TV shows for {year}-{month:02d} (from {start_date} to {end_date})...")

            while current_page <= total_pages_for_month:
                try:
                    # TMDB Discover API call with date filters
                    # sort_by can be adjusted, e.g., 'popularity.desc', 'first_air_date.asc'
                    response = discover.tv(
                        page=current_page,
                        first_air_date_gte=start_date,
                        first_air_date_lte=end_date,
                        sort_by='first_air_date.asc' # Sort by air date to ensure consistent pagination
                    )
                    total_api_calls += 1

                    if not response['results']:
                        print(f"No more results found for {year}-{month:02d} on page {current_page}.")
                        break # No results for this page, move to next month/year

                    total_pages_for_month = response['total_pages']
                    # TMDB API has a hard limit of 500 pages per query for discover.
                    # If total_pages_for_month exceeds 500, we will only get the first 500 pages.
                    if total_pages_for_month > 500:
                        total_pages_for_month = 500
                        print(f"WARNING: More than 500 pages for {year}-{month:02d}. Only fetching first 500 pages.")


                    for show in response['results']:
                        tv_show_id = show['id']
                        try:
                            # Use INSERT OR IGNORE to avoid inserting duplicate IDs
                            con.execute(f"INSERT OR IGNORE INTO tv_shows (id) VALUES ({tv_show_id});")
                            # Increment inserted_count for each attempted insertion.
                            # INSERT OR IGNORE handles uniqueness.
                            inserted_count += 1
                        except Exception as e:
                            print(f"ERROR: Could not insert TV show ID {tv_show_id} into database: {e}")
                            # Continue to next show even if one fails
                            continue
                    
                    current_page += 1

                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    minutes = int(elapsed_time // 60)
                    remaining_seconds = elapsed_time % 60
                    print(f"  Processed page {current_page-1}/{total_pages_for_month} for {year}-{month:02d}. Total unique IDs inserted: {inserted_count}. Total API calls: {total_api_calls}. Elapsed time: {minutes}m{remaining_seconds:.2f}s")
                    
                    # TMDB API has rate limits (e.g., 40 requests every 10 seconds).
                    # A delay of 0.25 seconds per request (4 requests per second) is generally safe.
                    time.sleep(0.25) 

                except requests.exceptions.HTTPError as e:
                    print(f"ERROR: HTTPError while fetching {year}-{month:02d}, page {current_page}: {e}")
                    if e.response.status_code == 429: # Too Many Requests
                        print("Rate limit hit. Waiting for 20 seconds before retrying this page...")
                        time.sleep(20)
                        # Do not increment current_page, retry the same page
                    else:
                        # For other HTTP errors, skip to next page/month
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

# Call the function to start populating TV show IDs
# You can adjust start_year and end_year as needed.
# Setting a wide range like 1936 to current year will take a very long time.
populate_all_tv_show_ids_by_date(start_year=1936, end_year=datetime.now().year)

# Close the DuckDB connection
con.close()