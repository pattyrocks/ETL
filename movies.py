import tmdbsimple as tmdb
import os
import duckdb
import pandas as pd
from datetime import date
import calendar

tmdb.API_KEY = os.getenv('TMDBAPIKEY')

con = duckdb.connect(database='tmdb',read_only=False)

con.execute('''CREATE /*OR REPLACE*/ TABLE movies (
    adult BOOLEAN,
    backdrop_path VARCHAR,
    genre_ids BIGINT[],
    id VARCHAR PRIMARY KEY,
    original_language VARCHAR,
    original_title VARCHAR,
    overview VARCHAR,
    popularity INTEGER,
    poster_path VARCHAR,
    release_date DATE,
    title VARCHAR,
    video BOOLEAN,
    vote_average FLOAT,
    vote_count INTEGER        
); ''')

def insert_into_movies():

    discover = tmdb.Discover()

    for year in range(1890,2026):

        for month in range(1,13):
                start_date = date(year, month, 1)
                end_date = date(year, month, calendar.monthrange(year, month)[1])

                response_pages = discover.movie(
                    primary_release_date_gte=start_date.strftime('%Y-%m-%d'),
                    primary_release_date_lte=end_date.strftime('%Y-%m-%d')
                )

                print(start_date)
                print(' ')
                print(end_date)

                for pg in range(1,response_pages['total_pages']+1):

                    response = discover.movie(
                        primary_release_date_gte=start_date.strftime('%Y-%m-%d'),
                        primary_release_date_lte=end_date.strftime('%Y-%m-%d'),
                        page=pg
                    )

                    result = response['results']
                    df = pd.DataFrame(result)

                    try:
                        con.execute("INSERT OR IGNORE INTO movies SELECT * FROM df")

                    except Exception as ex:
                        print(f'An error occurred: {ex} on year {year} and month {month}, page {pg}')
                        print(result)


    final_table = con.sql("SELECT * FROM movies")

    print(final_table)

    con.close()