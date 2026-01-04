# 🎬 ETL with Python: TMDB API → DuckDB

This project extracts **all movie and TV show data** from the [TMDB API](https://developer.themoviedb.org/docs), transforms it with pandas, and stores it in a local DuckDB database using analytics-ready schemas. It supports incremental updates, safe repeatable runs, and automated scheduling via GitHub Actions. It is designed to collect comprehensive movie, TV show, cast, and crew information for exploration, visualization, and interactive analysis.

---

## 🧭 Goal

Build a lightweight, ETL process to collect, clean, and store **all movies and TV shows available** on TMDB, along with their main cast and crew data. This will be the resource to a website featuring this data.

---

## 📦 Project Structure

| File | Description |
|---|---|
| `adding_movies_ids.py` | Parallelized script to fetch and store all movie IDs from TMDB (used as the base for movie ingestion). |
| `adding_tv_shows_ids.py` | Parallelized script to fetch and store all TV show IDs from TMDB (used as the base for TV ingestion). |
| `movies.py` | Extracts detailed movie data from the TMDB API and saves it to the `movies` table. |
| `movie_cast.py` | Extracts cast data for each movie and stores it in the `movie_cast` table. |
| `movie_crew.py` | Extracts crew data (e.g. directors, producers) for each movie and stores it in the `movie_crew` table. |
| `tv_shows.py` | Extracts detailed TV show information and updates the `tv_shows` table. |
| `tv_show_cast_crew.py` | Fetches aggregate cast and crew data for each TV show (using `/aggregate_credits`) and stores it in the `tv_show_cast_crew` table. |
| `update_job.py` | Incremental update job that consumes TMDB change feeds, fetches updated movie and TV data, and upserts records into existing tables. |
| `connection.py` | Centralized DuckDB connection helper used across all ingestion and update scripts. |
| `.github/workflows/weekly_update.yml` | GitHub Actions workflow that runs scheduled or manual pipeline updates (supports dry-run execution). |

---

## 🧰 Technologies

- Python (ETL orchestration)
- pandas (data transformation)
- requests (API ingestion)
- DuckDB (analytical storage)
- GitHub Actions (automation / CI)
- TMDB API

---

## 🔄 Incremental Update Strategy

- TMDB change endpoints are queried
- Only changed movie and TV IDs are processed
- Rows are upserted into DuckDB
- The pipeline is safe to run weekly

Example dry-run:
python update_job.py --dry-run --sample 10

---

## 🔧 How to Run

1. Clone the repository:
```
   git clone https://github.com/pattyrocks/ETL.git  
   cd ETL  
```
2. Install the requirements:
```
   pip install -r requirements.txt  
```
3. Set your TMDB API key as an environment variable:
```
   export TMDBAPIKEY=your_api_key  
```
4. Run the initial full load (first-time setup):
```
   python adding_movies_ids.py  
   python movies.py  
   python movie_cast.py  
   python movie_crew.py  
   python adding_tv_shows_ids.py  
   python tv_shows.py  
   python tv_show_cast_crew.py  
```
5. Run incremental updates (recommended for ongoing use):
```
   python update_job.py --sample 50  
```
   Optional dry-run preview (no database writes):
```
   python update_job.py --dry-run --sample 10  
```
6. (Optional) Automated runs:

   Incremental updates can also be executed on a schedule using GitHub Actions via
   ```
   .github/workflows/weekly_update.yml
   ```
---


## 🗃️ Output Tables

- `movies`  
  Fields: `id`, `title`, `release_date`, `vote_average`, `overview`, etc.  
  Grain: one row per movie

- `movie_cast`  
  Fields: `movie_id`, `person_id`, `name`, `character`, etc.  
  Grain: one row per movie–person–character

- `movie_crew`  
  Fields: `movie_id`, `person_id`, `name`, `job`, `department`, etc.  
  Grain: one row per movie–person–job

- `tv_shows`  
  Fields: `id`, `name`, `episode_run_time`, `homepage`, `status`, etc.  
  Grain: one row per TV show

- `tv_show_cast_crew`  
  Fields: `tv_id`, `person_id`, `name`, `character`, `roles`, `total_episode_count`, etc.  
  Grain: one row per TV show–person–role

---

## 🧪 In Progress

- website creation (**MVP** version)

---

## 📈 Future Improvements

- Add People (mainly actors) aggregated information
- Star schema or OBT modeling
- Normalize data into star schema (e.g. `dim_actor`, `dim_movie`, `dim_crew`, `dim_tv_show`) or OBT
- Implement orchestration with Airflow
- Enrich with marts for genres, production companies, episode-level data
- Add visualizations and interactive content to website

---

## 📎 Resources

- [TMDB API Documentation](https://developer.themoviedb.org/docs)
- [DuckDB Official Website](https://duckdb.org/)

---

> Built with AI by [Patricia Nascimento](https://www.linkedin.com/in/patricians) 👩🏻‍💻
