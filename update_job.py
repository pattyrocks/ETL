import time
import duckdb
import traceback
from datetime import datetime

from update.config import (
    DRY_RUN, SAMPLE_SIZE, DATABASE_PATH, args,
)
from update.utils import log_and_print
from update.schema import ensure_movies_table, ensure_tv_shows_table, ensure_cast_crew_tables
from update.discovery import discover_new_movie_ids, discover_new_tv_show_ids
from update.movies_info import update_movies_info
from update.tv_shows_info import update_tv_shows_info
from update.movie_cast import update_movie_cast
from update.movie_crew import update_movie_crew
from update.tv_show_cast import update_tv_show_cast
from update.tv_show_crew import update_tv_show_crew


def run_update_job():
    log_and_print("=" * 60)
    log_and_print("STARTING FULL UPDATE JOB")
    log_and_print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if DRY_RUN:
        log_and_print("[DRY RUN MODE] No writes will be made to the database")
    if SAMPLE_SIZE > 0:
        log_and_print(f"[SAMPLE MODE] Processing max {SAMPLE_SIZE} items per step")
    if args.skip_discover:
        log_and_print("[SKIP] ID discovery will be skipped")
    if args.skip_info:
        log_and_print("[SKIP] Info updates will be skipped")
    if args.skip_cast_crew:
        log_and_print("[SKIP] Cast/crew updates will be skipped")
    log_and_print("=" * 60)

    start_time = time.time()
    con = None

    try:
        con = duckdb.connect(database=DATABASE_PATH, read_only=DRY_RUN)
    except Exception as e:
        log_and_print(f"Failed to connect to database: {e}", level='error')
        return

    try:
        # Ensure all tables exist (skip in dry run since read_only)
        if not DRY_RUN:
            ensure_movies_table(con)
            ensure_tv_shows_table(con)
            ensure_cast_crew_tables(con)

        # Step 1: Discover new IDs from TMDB exports
        if not args.skip_discover:
            discover_new_movie_ids(con)
            discover_new_tv_show_ids(con)
        else:
            log_and_print("Skipping ID discovery (--skip-discover)")

        # Step 2: Update info for IDs missing details
        if not args.skip_info:
            update_movies_info(con)
            update_tv_shows_info(con)
        else:
            log_and_print("Skipping info updates (--skip-info)")

        # Step 3: Update cast/crew for IDs missing credits
        if not args.skip_cast_crew:
            update_movie_cast(con)
            update_movie_crew(con)
            update_tv_show_cast(con)
            update_tv_show_crew(con)
        else:
            log_and_print("Skipping cast/crew updates (--skip-cast-crew)")

    except Exception as e:
        log_and_print(f"Critical error in update job: {e}", level='error')
        log_and_print(traceback.format_exc(), level='error')
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass

    total_elapsed = time.time() - start_time
    hours, remainder = divmod(total_elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)

    log_and_print("=" * 60)
    log_and_print(f"FULL UPDATE JOB COMPLETE in {int(hours)}h {int(minutes)}m {seconds:.2f}s")
    log_and_print("=" * 60)


if __name__ == "__main__":
    run_update_job()
