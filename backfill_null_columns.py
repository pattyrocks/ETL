import os
import duckdb
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import HTTPError

API_KEY = os.getenv('TMDBAPIKEY')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
DATABASE_PATH = f'md:TMDB?motherduck_token={MOTHERDUCK_TOKEN}'

MAX_API_WORKERS = 10
BATCH_SIZE = 500
DEDUP_BATCH_SIZE = 10000

def fetch_credits_for_backfill(tv_id):
    """
    Fetches credits for the most recent season to get missing column data.
    Returns a dict mapping (tv_id, person_id) -> {credit_id, character, cast_order, cast_id}
    """
    try:
        import tmdbsimple as tmdb
        tmdb.API_KEY = API_KEY
        tv = tmdb.TV(tv_id)
        try:
            tv_info = tv.info()
        except HTTPError as e:
            if e.response.status_code == 404:
                # TV show not found in TMDB, skip silently
                return {}
            raise  # Re-raise other HTTP errors
        seasons = tv_info.get('seasons', [])
        if not seasons:
            return {}
        seasons_sorted = sorted(
            [s for s in seasons if s.get('season_number') is not None],
            key=lambda s: (s.get('air_date') or '', s['season_number']),
            reverse=True
        )
        if not seasons_sorted:
            return {}
        recent_season = seasons_sorted[0]
        season_number = recent_season['season_number']
        season = tmdb.TV_Seasons(tv_id, season_number)
        try:
            credits = season.credits()
        except HTTPError as e:
            if e.response.status_code == 404:
                return {}
            raise
        cast_list = credits.get('cast', [])
        
        result = {}
        for cast_member in cast_list:
            person_id = cast_member.get('id')
            roles = cast_member.get('roles')
            
            character = cast_member.get('character')
            if not character and roles and isinstance(roles, list) and len(roles) > 0:
                character = roles[0].get('character')
            
            cast_order = cast_member.get('order')
            if cast_order is None:
                cast_order = cast_member.get('cast_order')
            
            credit_id = cast_member.get('credit_id')
            if not credit_id and roles and isinstance(roles, list) and len(roles) > 0:
                credit_id = roles[0].get('credit_id')
            
            cast_id = cast_member.get('cast_id')
            if cast_id is None:
                cast_id = person_id
            
            result[(tv_id, person_id)] = {
                'credit_id': credit_id,
                'character': character,
                'cast_order': cast_order,
                'cast_id': cast_id
            }
        return result
    except Exception as e:
        print(f"Error fetching credits for tv_id {tv_id}: {e}")
        return {}


def check_and_remove_duplicates(con):
    """
    Checks for duplicate rows and removes them in batches.
    Duplicates are identified by all columns except inserted_at and updated_at.
    """
    print("Checking for duplicate rows...")
    
    dup_count = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT (
            tv_id, person_id, name, credit_id, character, cast_order, gender,
            profile_path, known_for_department, popularity, original_name, roles,
            total_episode_count, cast_id, also_known_as
        )) AS dup_count
        FROM tv_show_cast_crew
    """).fetchone()[0]
    
    if dup_count == 0:
        print("No duplicates found.")
        return
    
    print(f"Found approximately {dup_count} duplicate rows. Removing...")
    
    deleted_total = 0
    while True:
        # Count duplicates in this batch
        batch_count = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT
                    tv_id, person_id, name, credit_id, character, cast_order, gender,
                    profile_path, known_for_department, popularity, original_name, roles,
                    total_episode_count, cast_id, also_known_as
                FROM tv_show_cast_crew
                GROUP BY tv_id, person_id, name, credit_id, character, cast_order, gender,
                         profile_path, known_for_department, popularity, original_name, roles,
                         total_episode_count, cast_id, also_known_as
                HAVING COUNT(*) > 1
                LIMIT {DEDUP_BATCH_SIZE}
            )
        """).fetchone()[0]

        if batch_count == 0:
            break

        # Create a temp table with deduplicated rows
        con.execute("""
            CREATE OR REPLACE TEMP TABLE dedup_keep AS
            SELECT * FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY tv_id, person_id, name, credit_id, character, cast_order, gender,
                                     profile_path, known_for_department, popularity, original_name, roles,
                                     total_episode_count, cast_id, also_known_as
                        ORDER BY inserted_at
                    ) AS rn
                FROM tv_show_cast_crew
            )
            WHERE rn = 1
        """)
        
        # Get row counts
        before_count = con.execute("SELECT COUNT(*) FROM tv_show_cast_crew").fetchone()[0]
        
        # Replace table content
        con.execute("DELETE FROM tv_show_cast_crew")
        con.execute("""
            INSERT INTO tv_show_cast_crew
            SELECT tv_id, person_id, name, credit_id, character, cast_order, gender,
                   profile_path, known_for_department, popularity, original_name, roles,
                   total_episode_count, cast_id, also_known_as, inserted_at, updated_at
            FROM dedup_keep
        """)
        
        after_count = con.execute("SELECT COUNT(*) FROM tv_show_cast_crew").fetchone()[0]
        deleted_batch = before_count - after_count
        deleted_total += deleted_batch
        
        print(f"Removed {deleted_batch} duplicates in this batch.")
        
        # Drop temp table
        con.execute("DROP TABLE IF EXISTS dedup_keep")
        break  # Full dedup done in one pass

    print(f"Deduplication complete. Total duplicates removed: {deleted_total}")


def backfill_null_columns():
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)
    
    # Find rows with null columns that need backfilling
    print("Finding rows with null columns...")
    null_rows_df = con.execute("""
        SELECT DISTINCT tv_id 
        FROM tv_show_cast_crew 
        WHERE credit_id IS NULL 
           OR character IS NULL 
           OR cast_order IS NULL 
           OR cast_id IS NULL
    """).fetchdf()
    
    tv_ids_to_backfill = null_rows_df['tv_id'].tolist()
    total_to_backfill = len(tv_ids_to_backfill)
    
    if total_to_backfill == 0:
        print("No rows need backfilling.")
    else:
        print(f"Found {total_to_backfill} TV shows with null columns to backfill.")
        
        start_time = time.time()
        all_updates = {}
        processed_count = 0
        
        # Fetch data in parallel
        with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
            futures = {
                executor.submit(fetch_credits_for_backfill, tv_id): tv_id 
                for tv_id in tv_ids_to_backfill
            }
            for future in as_completed(futures):
                tv_id = futures[future]
                try:
                    result = future.result()
                    all_updates.update(result)
                    processed_count += 1
                    if processed_count % BATCH_SIZE == 0:
                        print(f"Fetched data for {processed_count}/{total_to_backfill} TV shows.")
                except Exception as e:
                    print(f"Error processing tv_id {tv_id}: {e}")
        
        print(f"Fetched data for {len(all_updates)} cast members.")
        
        # Update rows in batches
        print("Updating rows in database...")
        update_count = 0
        for (tv_id, person_id), data in all_updates.items():
            try:
                con.execute("""
                    UPDATE tv_show_cast_crew
                    SET 
                        credit_id = COALESCE(credit_id, ?),
                        character = COALESCE(character, ?),
                        cast_order = COALESCE(cast_order, ?),
                        cast_id = COALESCE(cast_id, ?)
                    WHERE tv_id = ? AND person_id = ?
                """, [
                    data['credit_id'],
                    data['character'],
                    data['cast_order'],
                    data['cast_id'],
                    tv_id,
                    person_id
                ])
                update_count += 1
                if update_count % 1000 == 0:
                    print(f"Updated {update_count} rows.")
            except Exception as e:
                print(f"Error updating tv_id={tv_id}, person_id={person_id}: {e}")
        
        end_time = time.time()
        print(f"Backfill complete. Updated {update_count} rows in {end_time - start_time:.2f} seconds.")
    
    # Run deduplication check
    check_and_remove_duplicates(con)
    
    con.close()
    print("Job finished.")


if __name__ == "__main__":
    backfill_null_columns()