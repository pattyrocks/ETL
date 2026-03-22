import os
import sys
import argparse
import logging
import duckdb
import tmdbsimple as tmdb
from datetime import datetime

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description='TMDB ETL Update Job')
parser.add_argument('--dry-run', action='store_true', help='Run without writing to database')
parser.add_argument('--sample', type=int, default=0, help='Process only N items per step (0 = all)')
parser.add_argument('--skip-discover', action='store_true', help='Skip ID discovery from TMDB exports')
parser.add_argument('--skip-info', action='store_true', help='Skip info update for movies/TV shows')
parser.add_argument('--skip-cast-crew', action='store_true', help='Skip cast/crew update')
args = parser.parse_args()

DRY_RUN = args.dry_run
SAMPLE_SIZE = args.sample

# --- Configuration ---
API_KEY = os.getenv('TMDBAPIKEY')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')

if not API_KEY:
    raise EnvironmentError("TMDBAPIKEY environment variable is not set")
if not MOTHERDUCK_TOKEN:
    raise EnvironmentError("MOTHERDUCK_TOKEN environment variable is not set")

DATABASE_PATH = f'md:TMDB?motherduck_token={MOTHERDUCK_TOKEN}'
tmdb.API_KEY = API_KEY

MAX_API_WORKERS = 15
DB_INSERT_BATCH_SIZE = 5000
API_BATCH_SIZE = 500
MAX_RETRIES = 3
RATE_LIMIT_RETRY_DELAY = 2

# --- Logging setup ---
_log_file = f"update_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(_log_file),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)
