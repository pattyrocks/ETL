# 🎬 TMDB ETL Pipeline: API → MotherDuck → Streamlit

A Python ETL pipeline that extracts **all movie and TV show data** from the [TMDB API](https://developer.themoviedb.org/docs), transforms it with pandas, and stores it in [MotherDuck](https://motherduck.com/) (cloud-hosted DuckDB). A [Streamlit](https://streamlit.io/) dashboard provides interactive data exploration.

The pipeline supports incremental updates, timestamped backups, safe repeatable runs, automated scheduling via GitHub Actions, and external disaster recovery to AWS S3.

🌐 **Live dashboard:** [tmdbetl.streamlit.app](https://tmdbetl.streamlit.app)

---

## 🧭 Goal

Build a lightweight, automated ETL pipeline to collect, clean, and store all movies and TV shows available on TMDB, along with their main cast and crew data — designed to power a public-facing website.

---

## 📁 File Structure

```
ETL/
├── .github/
│   └── workflows/
│       └── weekly_update.yml        # GitHub Actions: scheduled + manual runs
├── app/
│   ├── Home.py                      # Streamlit homepage
│   └── pages/
│       └── Top_10_Movies.py         # Top 10 movies page
├── backups/                         # Local backup directory (gitignored)
├── adding_movies_ids.py             # Initial load: fetch all movie IDs
├── adding_tv_shows_ids.py           # Initial load: fetch all TV show IDs
├── movies.py                        # Extract full movie details
├── movie_cast.py                    # Extract movie cast
├── movie_crew.py                    # Extract movie crew
├── tv_shows.py                      # Extract TV show details
├── tv_show_cast_crew.py             # Extract TV aggregate cast/crew
├── update_job.py                    # Incremental weekly update job
├── backup_to_glacier.py             # Backup MotherDuck DB to AWS S3
├── connection.py                    # MotherDuck connection helper
├── test_backup.py                   # Test backup with sample DuckDB DB
├── check_invalid_dates.py           # Audit invalid date values
├── clean_invalid_dates.py           # Fix invalid date values
├── convert_all_dates.py             # Date format migration utility
├── migrate_movies_table.py          # Schema migration helper
├── scan_integer_columns.py          # Column type audit utility
├── requirements.txt                 # Python dependencies
└── README.md
```

---

## 📦 Key Files

| File | Description |
|---|---|
| `adding_movies_ids.py` | Parallelized script to fetch and store all movie IDs from TMDB — used as the base for movie ingestion |
| `adding_tv_shows_ids.py` | Parallelized script to fetch and store all TV show IDs from TMDB — used as the base for TV ingestion |
| `movies.py` | Extracts detailed movie data and saves to the `movies` table |
| `movie_cast.py` | Extracts cast data per movie into the `movie_cast` table |
| `movie_crew.py` | Extracts crew data (directors, producers, etc.) into `movie_crew` |
| `tv_shows.py` | Extracts detailed TV show info into the `tv_shows` table |
| `tv_show_cast_crew.py` | Fetches aggregate cast/crew via `/aggregate_credits` into `tv_show_cast_crew` |
| `update_job.py` | Incremental update job — consumes TMDB change feeds, upserts records, creates in-DB timestamped backups before each run, and triggers S3 backup |
| `backup_to_glacier.py` | Backs up MotherDuck DB to AWS S3 (Glacier Flexible Retrieval, Mumbai region) before each ETL run |
| `connection.py` | Centralized MotherDuck/DuckDB connection helper |
| `test_backup.py` | Tests the backup flow using a local sample DuckDB database |

---

## 🧰 Technologies

- **Python** — ETL orchestration
- **pandas** — data transformation
- **requests** — TMDB API ingestion
- **DuckDB / MotherDuck** — analytical cloud data warehouse
- **AWS S3** — external disaster recovery backup (Glacier Flexible Retrieval)
- **Streamlit** — web dashboard
- **GitHub Actions** — automation and scheduling
- **TMDB API** — data source

---

## 🔄 Incremental Update Strategy

Every Friday at 03:00 UTC, GitHub Actions runs the update job:

1. **S3 backup** runs first — the current MotherDuck DB is backed up to S3 before any writes
2. **In-DB backup tables** are created in MotherDuck (timestamped snapshots for quick rollback)
3. TMDB `/changes` endpoints are queried for movies and TV shows updated since last run
4. Only changed IDs are fetched and processed
5. Records are upserted into MotherDuck
6. `last_run` timestamp is updated

```bash
# Test with a small sample before full run
python update_job.py --sample 10

# Force fetch changes from the last N days
python update_job.py --force 30

# Dry run (no DB writes, generates preview CSVs)
python update_job.py --dry-run
```

---

## 🗄️ Backup & Recovery Strategy

The pipeline has two independent backup layers:

| Layer | What it protects against | Recovery time |
|---|---|---|
| MotherDuck `_backup_TIMESTAMP` tables | ETL gone wrong (bad upsert, corrupted run) | Instant — query directly in MotherDuck |
| AWS S3 Glacier Flexible Retrieval | MotherDuck-level failure / catastrophic loss | Minutes–12 hours (external restore) |

**S3 configuration:**
- Region: `ap-south-1` (Mumbai)
- Storage class: Glacier Flexible Retrieval (`GLACIER`)
- Versioning: enabled (current + 1 previous version retained)
- Lifecycle policy: noncurrent versions expire after 7 days

**Testing the backup locally:**
```bash
python test_backup.py
```

---

## ⚙️ How to Run

### 1. Clone the repo
```bash
git clone https://github.com/pattyrocks/ETL.git
cd ETL
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set environment variables
```bash
export TMDBAPIKEY=your_tmdb_api_key
export MOTHERDUCK_TOKEN=your_motherduck_token
export AWS_ACCESS_KEY_ID=your_aws_key
export AWS_SECRET_ACCESS_KEY=your_aws_secret
export S3_BUCKET_NAME=your_bucket_name
```

### 4. Initial full load (first-time setup only)
```bash
python adding_movies_ids.py
python movies.py
python movie_cast.py
python movie_crew.py
python adding_tv_shows_ids.py
python tv_shows.py
python tv_show_cast_crew.py
```

### 5. Ongoing incremental updates
```bash
python update_job.py
```

### 6. Automated runs via GitHub Actions
Set the following repository secrets under **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `TMDBAPIKEY` | TMDB API key |
| `MOTHERDUCK_TOKEN` | MotherDuck access token |
| `AWS_ACCESS_KEY_ID` | AWS IAM key with S3 write access |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM secret |
| `S3_BUCKET_NAME` | S3 bucket name for backups |

The workflow runs automatically every Friday and can also be triggered manually with options for `dry_run` and `sample` size.

---

## 🌐 Running the Dashboard

```bash
streamlit run app/Home.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

### Deploying to Streamlit Cloud

1. Push your code to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Select your repo and branch
4. Set **Main file path** to `app/Home.py`
5. Under **Advanced settings → Secrets**, add:
```toml
MOTHERDUCK_TOKEN = "your_token"
```
6. Click **Deploy**

---

## 🗃️ Output Tables

| Table | Grain | Key fields |
|---|---|---|
| `movies` | One row per movie | `id`, `title`, `release_date`, `vote_average`, `overview` |
| `movie_cast` | One row per movie–person–character | `movie_id`, `person_id`, `name`, `character` |
| `movie_crew` | One row per movie–person–job | `movie_id`, `person_id`, `name`, `job`, `department` |
| `tv_shows` | One row per TV show | `id`, `status`, `number_of_seasons`, `last_air_date` |
| `tv_show_cast_crew` | One row per show–person–role | `tv_id`, `person_id`, `name`, `roles`, `total_episode_count` |
| `last_updates` | One row per ETL job | `job_name`, `last_run` |

All tables include `inserted_at` and `updated_at` audit timestamps.

---

## 🧪 In Progress / Future Improvements

- [ ] Star schema modeling (`dim_actor`, `dim_movie`, `dim_crew`, `dim_tv_show`)
- [ ] Enrich with marts for genres, production companies, episode-level data
- [ ] Orchestration with Airflow
- [ ] Expanded Streamlit dashboard with more visualizations
- [x] Streamlit dashboard MVP — live at [tmdbetl.streamlit.app](https://tmdbetl.streamlit.app)
- [x] Automated weekly updates via GitHub Actions
- [x] External S3 disaster recovery backup

---

## 📎 Resources

- [TMDB API Documentation](https://developer.themoviedb.org/docs)
- [DuckDB Official Website](https://duckdb.org/)
- [MotherDuck Documentation](https://motherduck.com/docs/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [AWS S3](https://aws.amazon.com/s3/)

---

> Built by [Patricia Nascimento](https://www.linkedin.com/in/patricians) 👩🏻‍💻
