# 🎬 TMDB ETL Pipeline: API → MotherDuck → Streamlit

A Python ETL pipeline that extracts **all movie and TV show data** from the [TMDB API](https://developer.themoviedb.org/docs), transforms it with pandas, and stores it in [MotherDuck](https://motherduck.com/) (cloud-hosted DuckDB). A [Streamlit](https://streamlit.io/) dashboard provides interactive data exploration.

The pipeline supports incremental updates, timestamped backups, safe repeatable runs, automated scheduling via GitHub Actions, and external disaster recovery to AWS S3.

🌐 **Live dashboard:** [tmdbetl.streamlit.app](https://tmdbetl.streamlit.app)

[![etl-update](https://github.com/pattyrocks/ETL/actions/workflows/etl-update.yml/badge.svg)](https://github.com/pattyrocks/ETL/actions/workflows/etl-update.yml)
[![s3-backup](https://github.com/pattyrocks/ETL/actions/workflows/s3-backup.yml/badge.svg)](https://github.com/pattyrocks/ETL/actions/workflows/s3-backup.yml)
[![monthly-pi-sync](https://github.com/pattyrocks/ETL/actions/workflows/monthly-pi-sync.yml/badge.svg)](https://github.com/pattyrocks/ETL/actions/workflows/monthly-pi-sync.yml)


---

## 🧭 Goal

Build a lightweight, automated ETL pipeline to collect, clean, and store all movies and TV shows available on TMDB, along with their main cast and crew data — designed to power a public-facing website.

---

## 📁 File Structure

```
ETL/
├── .github/
│   └── workflows/
│       ├── s3_backup.yml      # Backup only — runs every Thursday 03:00 UTC
│       └── etl-update.yml           # ETL only — runs every Friday 03:00 UTC
├── app/
│   ├── Home.py                      # Streamlit homepage
│   └── pages/
│       └── Top_10_Movies.py         # Top 10 movies page
├── backups/                         # Local backup directory (gitignored)
├── adding_movies_ids.py             # Initial load: fetch all movie IDs
├── adding_tv_shows_ids.py           # Initial load: fetch all TV show IDs
├── backup_to_glacier.py             # Backup MotherDuck DB to AWS S3
├── check_invalid_dates.py           # Audit invalid date values
├── clean_invalid_dates.py           # Fix invalid date values
├── connection.py                    # MotherDuck connection helper
├── convert_all_dates.py             # Date format migration utility
├── migrate_movies_table.py          # Schema migration helper
├── movie_cast.py                    # Extract movie cast
├── movie_crew.py                    # Extract movie crew
├── movies.py                        # Extract full movie details
├── scan_integer_columns.py          # Column type audit utility
├── test_backup.py                   # Backup inspection and testing tool
├── tv_show_cast.py                  # Extract TV show cast
├── tv_show_crew.py                  # Extract TV show crew
├── tv_shows.py                      # Extract TV show details
├── update_job.py                    # Incremental weekly update job
├── requirements.txt                 # Python dependencies
├── tests.md                         # Manual test notes
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
| `tv_show_cast.py` | Fetches cast data via season credits into `tv_show_cast` |
| `tv_show_crew.py` | Fetches crew data via season credits into `tv_show_crew` |
| `update_job.py` | Incremental update job — consumes TMDB change feeds, upserts records with progress logging and time forecasting |
| `backup_to_glacier.py` | Backs up MotherDuck `TMDB` → `TMDB_backup` (timestamped tables) and uploads local `.db` file to AWS S3 |
| `connection.py` | Centralized MotherDuck/DuckDB connection helper |
| `test_backup.py` | Interactive backup tool — test local `.db` files, compare with live MotherDuck, inspect `TMDB_backup` tables |

---

## 🧰 Technologies

- **Python** — ETL orchestration
- **pandas** — data transformation
- **requests** — TMDB API ingestion
- **DuckDB / MotherDuck** — analytical cloud data warehouse
- **AWS S3** — external disaster recovery backup (Glacier Flexible Retrieval, Mumbai `ap-south-1`)
- **Streamlit** — web dashboard
- **GitHub Actions** — automation and scheduling
- **TMDB API** — data source

---

## 🔄 Incremental Update Strategy

The pipeline runs on a **split schedule** to manage MotherDuck compute limits:

| Day | Workflow | What it does |
|---|---|---|
| Thursday 03:00 UTC | `thursday_backup.yml` | Backs up MotherDuck → S3 |
| Friday 03:00 UTC | `etl-update.yml` | Runs incremental ETL update |

The Friday workflow **verifies a recent S3 backup exists** before running — if no backup is found or it's older than 2 days, the ETL is halted for safety.

**Update flow:**
1. TMDB `/changes` endpoints queried for movies and TV shows updated since last run
2. Only changed IDs are fetched and processed (capped at 2,000 per type per run)
3. Progress logged at 25%, 50%, 75%, 100% with elapsed time, remaining estimate and ETA
4. Records upserted into MotherDuck
5. `last_run` timestamp updated

```bash
# Test with a small sample
python update_job.py --sample 10

# Force fetch changes from the last N days
python update_job.py --force 30

# Dry run (no DB writes, generates preview CSVs)
python update_job.py --dry-run
```

---

## 🗄️ Backup & Recovery Strategy

The pipeline has three independent backup layers:

| Layer | Where | What it protects against | Recovery time |
|---|---|---|---|
| `md:TMDB_backup` timestamped tables | Separate MotherDuck DB | ETL corruption, bad upsert | Instant — query directly |
| AWS S3 Glacier Flexible Retrieval | Mumbai `ap-south-1` | MotherDuck-level failure | Minutes–12 hours |
| S3 versioning | Same S3 bucket | Bad backup overwriting good one | Via S3 console |

**S3 configuration:**
- Region: `ap-south-1` (Mumbai)
- Storage class: Glacier Flexible Retrieval (`GLACIER`)
- Versioning: enabled
- Lifecycle policy: noncurrent versions expire after 7 days

**`TMDB_backup` database structure:**
Each Thursday backup copies live tables with a timestamp suffix (e.g. `movies_20260219_163858`). Previous timestamped tables are only deleted after new ones are created successfully — safe atomic rotation.

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
python tv_show_cast.py
python tv_show_crew.py
```

### 5. Ongoing incremental updates
```bash
python update_job.py
```

### 6. Manual backup
```bash
python backup_to_glacier.py
```

### 7. Automated runs via GitHub Actions
Set the following repository secrets under **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `TMDBAPIKEY` | TMDB API key |
| `MOTHERDUCK_TOKEN` | MotherDuck access token |
| `AWS_ACCESS_KEY_ID` | AWS IAM key with S3 write access |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM secret |
| `S3_BUCKET_NAME` | S3 bucket name for backups |

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
| `tv_show_cast` | One row per show–person–character | `tv_id`, `person_id`, `name`, `roles`, `total_episode_count` |
| `tv_show_crew` | One row per show–person–job | `tv_id`, `person_id`, `name`, `job`, `department` |
| `last_updates` | One row per ETL job | `job_name`, `last_run` |

All tables include `inserted_at` and `updated_at` audit timestamps.

---

## 🧪 In Progress / Future Improvements

- [x] Streamlit dashboard MVP — live at [tmdbetl.streamlit.app](https://tmdbetl.streamlit.app)
- [x] Automated weekly updates via GitHub Actions (split Thu/Fri to manage compute limits)
- [x] External S3 disaster recovery backup (Glacier Flexible Retrieval, Mumbai)
- [x] `TMDB_backup` MotherDuck mirror database with timestamped table rotation
- [x] S3 backup verification before ETL runs
- [x] Progress logging with time forecast and ETA
- [ ] Add People/Actors aggregated information table
- [ ] Star schema modeling (`dim_actor`, `dim_movie`, `dim_crew`, `dim_tv_show`)
- [ ] Orchestration with Airflow
- [ ] Expanded Streamlit dashboard with more visualizations
- [ ] Self-hosted DuckDB on Raspberry Pi 5 via Docker (eliminate MotherDuck compute limits)

---

## 📎 Resources

- [TMDB API Documentation](https://developer.themoviedb.org/docs)
- [DuckDB Official Website](https://duckdb.org/)
- [MotherDuck Documentation](https://motherduck.com/docs/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [AWS S3 Pricing](https://aws.amazon.com/s3/pricing/)

---

> Built by [Patricia Nascimento](https://www.linkedin.com/in/patricians) 👩🏽‍💻