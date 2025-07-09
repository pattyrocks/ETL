# 🎬 ETL with Python: TMDB API → DuckDB

This project extracts **all movie data** from the [TMDB API](https://developer.themoviedb.org/docs), transforms it using `pandas`, and stores it locally in a `DuckDB` database. It is a modular ETL pipeline built with Python, designed to collect comprehensive movie, cast, and crew information for analysis and exploration.

---

## 🧭 Goal

Build a lightweight, fully local ETL process to collect, clean, and store **all movies available** on TMDB, along with their main cast and crew data — and prepare the pipeline to include TV shows.

---

## 📦 Project Structure

| 📄 File                        | 📝 Description                                                                 |
|-------------------------------|--------------------------------------------------------------------------------|
| `movies.py`                   | Extracts all movies from TMDB API and saves them to the `movies` table.        |
| `movie_cast.py`               | Extracts cast data for each movie and saves it to the `movie_cast` table.      |
| `movie_crew.py`               | Extracts crew data (e.g. directors, producers) and stores in `movie_crew`.     |
| `adding_tv_shows_ids.py`      | (WIP) Script to extend the pipeline to include TV shows.                       |

---

## 🧰 Technologies

- Python
- requests
- DuckDB
- TMDB API

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
   export TMDB_API_KEY=your_api_key
   ```

4. Run the scripts:
   ```
   python movies.py
   python movie_cast.py
   python movie_crew.py
   ```
---

## 🗃️ Output Tables

- `movies`  
  Fields: `id`, `title`, `release_date`, `vote_average`, `overview`, etc.

- `movie_cast`  
  Fields: `movie_id`, `actor_id`, `actor_name`, `character`

- `movie_crew`  
  Fields: `movie_id`, `crew_id`, `crew_name`, `job`, `department`

---

## 🧪 In Progress

- `adding_tv_shows_ids.py`: expanding the scope to include TV series in the same DuckDB pipeline

---

## 📈 Future Improvements

- Normalize data into star schema (e.g. `dim_actor`, `dim_movie`, `dim_crew`) or OBT
- Schedule runs with cron or Apache Airflow
- Enrich with marts for genres, production companies, episode-level data
- Visualizations using a vizualition tool

---

## 📎 Resources

- [TMDB API Documentation](https://developer.themoviedb.org/docs)
- [DuckDB Official Website](https://duckdb.org/)

---

> Built by [Patricia Nascimento](https://www.linkedin.com/in/patricians) 👩🏻‍💻
