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
con = duckdb.connect(database='TMDB', read_only=False)

con.disconnect()

con.close()