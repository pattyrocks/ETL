# Test plan and commands — verify update_job.py without touching production tables

## Purpose
- Verify update_job.py behaviour safely before running real updates.
- Use dry-run and in-memory modes, small samples, and a temporary test DB copy.

## Prerequisites
- Set TMDB API key in shell:
```zsh
export TMDBAPIKEY=your_tmdb_key
```
- Install project deps (from repo root):
```zsh
pip install -r requirements.txt
```

## Quick smoke tests (fast, no DB writes)

### 1) Dry-run (no DB writes, saves preview CSVs to /tmp)
- Command:
```zsh
python update_job.py --dry-run --sample 10
```
- Check previews:
```zsh
ls /tmp/update_job_*_preview_*.csv
open /tmp/update_job_movies_preview_*.csv   # macOS: opens CSV viewer
```

### 2) In-memory full flow (exercises table/schema logic, nothing persisted)
- Command:
```zsh
python update_job.py --in-memory --sample 20
```

### 3) Dry-run without sample (try full change-list but still no writes)
- Command:
```zsh
python update_job.py --dry-run
```

## Test against a temporary test DB (safe full upsert)

### 1) Copy production DB file
```zsh
cp TMDB TMDB_test
```

### 2) Temporarily point the updater to TMDB_test and run (safe full upsert)
- Make a backup and edit DB_PATH in update_job.py (temporary):
```zsh
cp update_job.py update_job.py.bak
sed -i.bak "s/DB_PATH = 'TMDB'/DB_PATH = 'TMDB'/" update_job.py
```
- Run limited sample to verify upserts:
```zsh
python update_job.py --sample 50
```
- Inspect test DB with duckdb CLI:
```zsh
duckdb TMDB_test "SELECT COUNT(*) FROM movies;"
duckdb TMDB_test "SELECT COUNT(*) FROM movie_cast;"
duckdb TMDB_test "SELECT COUNT(*) FROM movie_crew;"
duckdb TMDB_test "SELECT COUNT(*) FROM tv_shows;"
duckdb TMDB_test "SELECT COUNT(*) FROM tv_show_cast_crew;"
```
- Restore updater:
```zsh
mv update_job.py.bak update_job.py
```

> Note: `sed -i.bak` keeps a backup — adjust if your sed behaves differently on macOS (use `-i '' -e` pattern).

## Verify last-run logic (dry-run vs real)
- In dry-run mode the job will not update `last_updates`.
- Inspect `last_updates` in the test DB:
```zsh
duckdb TMDB_test "SELECT * FROM last_updates;"
```

## Manual GitHub Actions test (optional)
- After committing update_job.py & workflow, trigger manually:
```bash
gh workflow run weekly-tmdb-update --ref main
```

## Checks to perform for each test
- No unexpected exceptions printed to console.
- CSV preview headers contain expected columns.
- Values look sane (ids, names, timestamps).
- In test DB runs verify tables exist and row counts updated.
- Confirm `last_updates` timestamp only changes on non-dry runs.

## If anything fails
- Re-run with a very small sample to isolate:
```zsh
python update_job.py --dry-run --sample 5
```
- Inspect the preview CSV for the problematic row.
- Use logs to identify API failures or schema mismatches.

## Quick helper commands summary
```bash
# Install deps
pip install -r requirements.txt

# Dry-run sample
python update_job.py --dry-run --sample 10

# In-memory run
python update_job.py --in-memory --sample 20

```zsh
pip install -r requirements.txt

# Dry-run sample
python update_job.py --dry-run --sample 10

# In-memory run
python update_job.py --in-memory --sample 20

# Copy DB
cp TMDB TMDB_test

# Point updater to test DB (temporary)
sed -i.bak "s/DB_PATH = 'TMDB'/DB_PATH = 'TMDB'/" update_job.py

# Run against test DB
python update_job.py --sample 50

# Inspect test DB
duckdb TMDB_test "SELECT COUNT(*) FROM movies;"

# Restore updater
mv update_job.py.bak update_job.py

# Trigger workflow manually
gh workflow run weekly-tmdb-update --ref main
```