# TMDB ETL Pipeline

Python ETL pipeline that extracts movie and TV show data from the [TMDB API](https://developer.themoviedb.org/docs), loads it into [MotherDuck](https://motherduck.com/) (cloud DuckDB), and serves a [Streamlit](https://streamlit.io/) dashboard.

**Live dashboard:** [tmdbetl.streamlit.app](https://tmdbetl.streamlit.app)

[![etl-update](https://github.com/pattyrocks/ETL/actions/workflows/etl-update.yml/badge.svg)](https://github.com/pattyrocks/ETL/actions/workflows/etl-update.yml)
[![s3-backup](https://github.com/pattyrocks/ETL/actions/workflows/s3-backup.yml/badge.svg)](https://github.com/pattyrocks/ETL/actions/workflows/s3-backup.yml)

---

## Architecture

```
TMDB Daily Exports ──► discovery.py ──► movies / tv_shows (IDs only)
                                              │
TMDB API ──► movies_info.py / tv_shows_info.py ──► movies / tv_shows (full details)
                                              │
TMDB API ──► movie_cast.py / movie_crew.py ──► movie_cast / movie_crew
             tv_show_cast.py / tv_show_crew.py ──► tv_show_cast / tv_show_crew
                                              │
                                         MotherDuck
```

**Orchestrator:** `update_job.py` runs all steps in sequence: discover → info → cast/crew.

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.11 |
| Database | DuckDB / MotherDuck |
| API | TMDB API (via `tmdbsimple`) |
| Transforms | pandas |
| Dashboard | Streamlit |
| Scheduling | GitHub Actions |
| Backup | AWS S3 (Glacier Flexible Retrieval) |

---

## Pipeline Steps

| Step | Module | Description |
|---|---|---|
| 1. Discover IDs | `discovery.py` | Downloads TMDB daily export files, inserts new movie/TV show IDs into DB |
| 2. Update movie info | `movies_info.py` | Fetches details from TMDB API for movies missing info, deduplicates by `id` |
| 3. Update TV show info | `tv_shows_info.py` | Fetches details from TMDB API for TV shows missing info, deduplicates by `id` |
| 4. Movie cast/crew | `movie_cast.py`, `movie_crew.py` | Fetches credits for movies missing cast/crew data |
| 5. TV show cast/crew | `tv_show_cast.py`, `tv_show_crew.py` | Fetches credits for TV shows missing cast/crew data |

Each step records a row in `last_updates` for audit history.

**Dead ID handling:** IDs that return 404 from TMDB (deleted/merged entries) are automatically purged from the database. Samples are randomized to avoid repeatedly hitting the same dead IDs.

---

## Database Tables

| Table | Grain | Key Columns |
|---|---|---|
| `movies` | One row per movie | `id` (PK), `title`, `release_date`, `vote_average`, `revenue`, `runtime`, `status` |
| `tv_shows` | One row per TV show | `id` (PK), `name`, `status`, `number_of_seasons`, `number_of_episodes`, `popularity` |
| `movie_cast` | Movie–person–character | `movie_id`, `person_id`, `character`, `surrogate_key` |
| `movie_crew` | Movie–person–job | `movie_id`, `person_id`, `job`, `department`, `surrogate_key` |
| `tv_show_cast` | Show–person–character | `tv_id`, `person_id`, `character`, `roles`, `surrogate_key` |
| `tv_show_crew` | Show–person–job | `tv_id`, `person_id`, `roles`, `total_episode_count`, `surrogate_key` |
| `last_updates` | One row per table per run | `table_name`, `last_run`, `surrogate_key` |

All tables include `inserted_at` and `updated_at` timestamps.

---

## GitHub Actions

### `etl-update.yml` — Daily at 03:00 UTC

- Default `--sample 5000` for scheduled runs (configurable via manual dispatch)
- Verifies S3 backup exists before running
- Restores/uploads checkpoint files (`.pkl`) between runs for incremental progress
- 6-hour timeout

### `s3-backup.yml` — Weekly backup

- Copies MotherDuck DB to `TMDB_backup` (timestamped tables)
- Uploads local `.db` export to S3 Glacier Flexible Retrieval

---

## Usage

### Environment Variables

```bash
export TMDBAPIKEY=your_tmdb_api_key
export MOTHERDUCK_TOKEN=your_motherduck_token
export AWS_ACCESS_KEY_ID=your_aws_key
export AWS_SECRET_ACCESS_KEY=your_aws_secret
export S3_BUCKET_NAME=your_bucket_name
```

### CLI

```bash
# Full run (all steps)
python update_job.py

# Limit to N items per step
python update_job.py --sample 100

# Dry run (read-only, no DB writes)
python update_job.py --dry-run

# Skip specific steps
python update_job.py --skip-discover
python update_job.py --skip-info
python update_job.py --skip-cast-crew
```

### Initial Load (first-time only)

```bash
python adding_movies_ids.py
python adding_tv_shows_ids.py
python movies.py
python tv_shows.py
python movie_cast.py
python movie_crew.py
python tv_show_cast.py
python tv_show_crew.py
```

### Dashboard

```bash
streamlit run app/Home.py
```

---

## GitHub Actions Secrets

| Secret | Description |
|---|---|
| `TMDBAPIKEY` | TMDB API key |
| `MOTHERDUCK_TOKEN` | MotherDuck access token |
| `AWS_ACCESS_KEY_ID` | AWS IAM key with S3 write access |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM secret |
| `S3_BUCKET_NAME` | S3 bucket name for backups |

---

## File Structure

```
ETL/
├── update_job.py          # Main orchestrator
├── config.py              # CLI args, env vars, logging setup
├── schema.py              # CREATE TABLE definitions
├── connection.py          # MotherDuck connection helper
├── discovery.py           # ID discovery from TMDB daily exports
├── movies_info.py         # Movie detail fetcher
├── tv_shows_info.py       # TV show detail fetcher
├── movie_cast.py          # Movie cast fetcher
├── movie_crew.py          # Movie crew fetcher
├── tv_show_cast.py        # TV show cast fetcher
├── tv_show_crew.py        # TV show crew fetcher
├── tv_show_cast_crew.py   # Shared TV show credits logic
├── dedup.py               # Duplicate removal utility
├── utils.py               # Logging, checkpoints, surrogate keys, purge
├── backup_to_glacier.py   # S3 backup script
├── backfill_null_columns.py  # Manual null-fill utility
├── app/
│   ├── Home.py            # Streamlit homepage
│   └── pages/
│       └── Top_10_Movies.py
└── .github/workflows/
    ├── etl-update.yml     # Daily ETL (03:00 UTC)
    └── s3-backup.yml      # Weekly S3 backup
```

---

> Built by 👩🏽‍💻 [Patricia Nascimento](https://www.linkedin.com/in/patricians)