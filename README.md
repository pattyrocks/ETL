# 🎬 ETL with Python: TMDB API → DuckDB

This project extracts **all movie and TV show data** from the [TMDB API](https://developer.themoviedb.org/docs), transforms it using `pandas`, and stores it locally in a `DuckDB` database. It is a modular ETL pipeline built with Python, designed to collect comprehensive movie, TV show, cast, and crew information for and exploration.

---

## 🧭 Goal

Build a lightweight, fully local ETL process to collect, clean, and store **all movies and TV shows available** on TMDB, along with their main cast and crew data.

---

## 📦 Project Structure

| 📄 File                        | 📝 Description                                                                 |
|-------------------------------|--------------------------------------------------------------------------------|
| `adding_movies_ids.py`         | Parallelized script to fetch and store all movie IDs from TMDB.                |
| `movies.py`                   | Extracts data from TMDB API and saves them to the `movies` table.              |
| `movie_cast.py`               | Extracts cast data for each movie and saves it to the `movie_cast` table.      |
| `movie_crew.py`               | Extracts crew data (e.g. directors, producers) and stores in `movie_crew`.     |
| `adding_tv_shows_ids.py`      | Parallelized script to fetch and store all TV show IDs from TMDB.              |
| `tv_shows.py`                 | Extracts detailed info for each TV show and updates the `tv_shows` table.      |
| `tv_show_cast_crew.py`        | Fetches aggregate cast data for each TV show (using `/aggregate_credits`) and saves it to the `tv_show_cast_crew` table. |

---

## 🧰 Technologies

- Python
- requests
- pandas
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
   export TMDBAPIKEY=your_api_key
   ```

4. Run the scripts:
   ```
   python adding_movies_ids.py
   python movies.py
   python movie_cast.py
   python movie_crew.py
   python adding_tv_shows_ids.py
   python tv_shows.py
   python tv_show_cast_crew.py
   ```

---

## 🗃️ Output Tables

- `movies`  
  Fields: `id`, `title`, `release_date`, `vote_average`, `overview`, etc.

- `movie_cast`  
  Fields: `movie_id`, `person_id`, `name`, `character`, etc.

- `movie_crew`  
  Fields: `movie_id`, `person_id`, `name`, `job`, `department`, etc.

- `tv_shows`  
  Fields: `id`, `name`, `episode_run_time`, `homepage`, `status`, etc.

- `tv_show_cast_crew`  
  Fields: `tv_id`, `person_id`, `name`, `character`, `roles`, `total_episode_count`, etc.

---

## 🧪 In Progress

- Incremental/delta updates for new TMDB content
- Further normalization of TV show crew and cast data

---

## 📈 Future Improvements

- Add People (mainly actors) aggregated information
- Normalize data into star schema (e.g. `dim_actor`, `dim_movie`, `dim_crew`, `dim_tv_show`) or OBT
- Schedule runs with cron or Apache Airflow
- Enrich with marts for genres, production companies, episode-level data
- Visualizations

---

## 📎 Resources

- [TMDB API Documentation](https://developer.themoviedb.org/docs)
- [DuckDB Official Website](https://duckdb.org/)

---

> Built with AI by [Patricia Nascimento](https://www.linkedin.com/in/patricians) 👩🏻‍💻
