import tmdbsimple as tmdb
import os
import duckdb
import pandas as pd
import time
import requests


tmdb.API_KEY = os.getenv('TMDBAPIKEY')

con = duckdb.connect(database='tmdb',read_only=False)

processed_count = 0
skipped_ids = []

start_time = time.time()

def add_info_to_movies():

    i = 1

    movies_ids = con.sql('''SELECT id FROM movies''').fetchdf()

    print("Starting movie data retrieval...\n")

    for movie_id in movies_ids['id']:
        try:
            print(f"Attempting to fetch data for movie ID: {movie_id}")

            dictionary = tmdb.Movies(movie_id).info()

            df = pd.DataFrame.from_dict(dictionary, orient='index')
            df = df.transpose()

            df = df[['belongs_to_collection', 'budget', 'homepage', 'id',
                 'origin_country', 'revenue', 'runtime', 'status',
                 'production_countries']]
            
            con.execute('''UPDATE movies 
                           SET
                            belongs_to_collection = df.belongs_to_collection,
                            budget = df.budget,
                            homepage = df.homepage,
                            origin_country = df.origin_country,
                            revenue = df.revenue,
                            runtime = df.runtime,
                            status = df.status,
                            production_countries = df.production_countries
                           FROM df 
                           WHERE df.id = movies.id''')
            i += 1

            end_time = time.time()
            elapsed_time = end_time - start_time
            minutes = int(elapsed_time // 60) # Integer division for whole minutes
            remaining_seconds = elapsed_time % 60 # Modulo for remaining seconds     

            print(f'I have inserted data to id {movie_id} and I have processed {i} registers in {minutes}m{remaining_seconds:.2f}s')

        except requests.exceptions.HTTPError as e:
            print(f"ERROR: HTTPError for ID {movie_id}: {e}")
            skipped_ids.append(movie_id)
            continue

        except KeyError as e:
            print(f"ERROR: KeyError for ID {movie_id} - Missing data: {e}. DataFrame conversion might fail.")
            skipped_ids.append(movie_id)
            continue

        except Exception as e:
            print(f"ERROR: An unexpected error occurred for ID {movie_id}: {e}")
            skipped_ids.append(movie_id)
            continue

    print(f"\nProcessing complete.")
    print(f"Successfully processed {processed_count} movies.")

if skipped_ids:
    print(f"Skipped IDs due to errors: {skipped_ids}")

    con.close()