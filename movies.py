import tmdbsimple as tmdb
import os
import duckdb
import pandas as pd
import time
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

tmdb.API_KEY = os.getenv('TMDBAPIKEY')

# --- logging setup ---
_log_file = f"movies_diagnostics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

con = duckdb.connect(database='TMDB', read_only=False)

columns_to_add = [
    "adult BOOLEAN",
    "backdrop_path VARCHAR",
    "belongs_to_collection VARCHAR",
    "budget BIGINT",
    "genres VARCHAR",
    "homepage VARCHAR",
    "imdb_id VARCHAR",
    "origin_country VARCHAR[]",
    "original_language VARCHAR",
    "original_title VARCHAR",
    "overview VARCHAR",
    "popularity DOUBLE",
    "poster_path VARCHAR",
    "production_companies VARCHAR",
    "production_countries VARCHAR",
    "release_date DATE",
    "revenue BIGINT",
    "runtime INTEGER",
    "spoken_languages VARCHAR",
    "status VARCHAR",
    "tagline VARCHAR",
    "title VARCHAR",
    "video BOOLEAN",
    "vote_average DOUBLE",
    "vote_count INTEGER"
]

for col in columns_to_add:
    con.execute(f"ALTER TABLE movies ADD COLUMN IF NOT EXISTS {col};")

processed_count = 0
skipped_ids = []
start_time = time.time()

def fetch_movie_info(movie_id):
    try:
        movie_info = tmdb.Movies(movie_id).info()
        df = pd.DataFrame.from_dict(movie_info, orient='index').transpose()
        # Select all attributes from the info dict
        selected_df = df.copy()

        # Convert 'production_countries' list of dicts to a JSON string for storage
        if 'production_countries' in selected_df.columns and not selected_df['production_countries'].empty:
            selected_df.loc[:, 'production_countries'] = selected_df['production_countries'].apply(
                lambda x: str(x) if isinstance(x, list) else None
            )
        else:
            selected_df.loc[:, 'production_countries'] = None

        if 'origin_country' in selected_df.columns and not selected_df['origin_country'].empty:
            selected_df.loc[:, 'origin_country'] = selected_df['origin_country'].apply(
                lambda x: [str(item) for item in x] if isinstance(x, list) else None
            )
        else:
            selected_df.loc[:, 'origin_country'] = None

        # You can add similar conversions for other list/dict attributes if needed

        return selected_df
    except requests.exceptions.HTTPError as e:
        log_and_print(f"ERROR: HTTPError for movie ID {movie_id}: {e}", level='error')
        return ('http', movie_id)
    except KeyError as e:
        log_and_print(f"ERROR: KeyError for movie ID {movie_id} - Missing data: {e}. DataFrame conversion might fail.", level='error')
        return ('key', movie_id)
    except Exception as e:
        log_and_print(f"ERROR: An unexpected error occurred for movie ID {movie_id}: {e}", level='error')
        return ('other', movie_id)

def add_info_to_movies_parallel(max_workers=8):
    global processed_count
    movies_ids_df = con.sql('''SELECT id FROM movies''').fetchdf()

    if movies_ids_df.empty:
        log_and_print("No movie IDs found in the 'movies' table. Please populate it first.")
        return

    log_and_print("Starting movie data retrieval...\n")

    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for movie_id in movies_ids_df['id']:
            futures.append(executor.submit(fetch_movie_info, movie_id))

        for future in as_completed(futures):
            result = future.result()
            if isinstance(result, tuple):
                # Error occurred, add to skipped_ids
                skipped_ids.append(result[1])
                continue
            selected_df = result
            try:
                con.execute(f'''
                    UPDATE movies
                    SET
                        adult = selected_df.adult,
                        backdrop_path = selected_df.backdrop_path,
                        belongs_to_collection = selected_df.belongs_to_collection,
                        budget = selected_df.budget,
                        genres = selected_df.genres,
                        homepage = selected_df.homepage,
                        imdb_id = selected_df.imdb_id,
                        origin_country = selected_df.origin_country,
                        original_language = selected_df.original_language,
                        original_title = selected_df.original_title,
                        overview = selected_df.overview,
                        popularity = selected_df.popularity,
                        poster_path = selected_df.poster_path,
                        production_companies = selected_df.production_companies,
                        production_countries = selected_df.production_countries,
                        release_date = selected_df.release_date,
                        revenue = selected_df.revenue,
                        runtime = selected_df.runtime,
                        spoken_languages = selected_df.spoken_languages,
                        status = selected_df.status,
                        tagline = selected_df.tagline,
                        title = selected_df.title,
                        video = selected_df.video,
                        vote_average = selected_df.vote_average,
                        vote_count = selected_df.vote_count
                    FROM selected_df
                    WHERE movies.id = selected_df.id
                ''')
                processed_count += 1
                end_time = time.time()
                elapsed_time = end_time - start_time
                minutes = int(elapsed_time // 60)
                remaining_seconds = elapsed_time % 60
                log_and_print(f'Successfully updated data for movie ID {selected_df["id"].values[0]}. Processed {processed_count} records in {minutes}m{remaining_seconds:.2f}s')
            except Exception as e:
                log_and_print(f"ERROR: Could not update movie ID {selected_df['id'].values[0]}: {e}", level='error')
                skipped_ids.append(selected_df['id'].values[0])
                continue

    log_and_print(f"\nProcessing complete.")
    log_and_print(f"Successfully processed {processed_count} movies.")

    if skipped_ids:
        log_and_print(f"Skipped IDs due to errors: {skipped_ids}", level='warning')

add_info_to_movies_parallel()