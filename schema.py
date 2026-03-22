def ensure_movies_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id BIGINT PRIMARY KEY,
            adult BOOLEAN,
            backdrop_path VARCHAR,
            belongs_to_collection VARCHAR,
            budget BIGINT,
            genres VARCHAR,
            homepage VARCHAR,
            imdb_id VARCHAR,
            origin_country VARCHAR[],
            original_language VARCHAR,
            original_title VARCHAR,
            overview VARCHAR,
            popularity DOUBLE,
            poster_path VARCHAR,
            production_companies VARCHAR,
            production_countries VARCHAR,
            release_date DATE,
            revenue BIGINT,
            runtime INTEGER,
            spoken_languages VARCHAR,
            status VARCHAR,
            tagline VARCHAR,
            title VARCHAR,
            video BOOLEAN,
            vote_average DOUBLE,
            vote_count INTEGER,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def ensure_tv_shows_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS tv_shows (
            id BIGINT PRIMARY KEY,
            name VARCHAR,
            episode_run_time VARCHAR[],
            in_production BOOLEAN,
            popularity INTEGER,
            last_air_date VARCHAR,
            number_of_episodes DOUBLE,
            number_of_seasons DOUBLE,
            origin_country VARCHAR[],
            production_countries VARCHAR,
            status VARCHAR,
            type VARCHAR,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def ensure_cast_crew_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS tv_show_cast (
            tv_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            character VARCHAR,
            cast_order INTEGER,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            roles VARCHAR,
            total_episode_count INTEGER,
            cast_id BIGINT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            surrogate_key VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS tv_show_crew (
            tv_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            character VARCHAR,
            cast_order INTEGER,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            roles VARCHAR,
            total_episode_count INTEGER,
            cast_id BIGINT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            surrogate_key VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS movie_cast (
            movie_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            character VARCHAR,
            cast_order INTEGER,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            cast_id BIGINT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            surrogate_key VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS movie_crew (
            movie_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            gender INTEGER,
            known_for_department VARCHAR,
            popularity DOUBLE,
            adult BOOLEAN,
            department VARCHAR,
            job VARCHAR,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            surrogate_key VARCHAR
        );
    """)


def ensure_last_updates_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS last_updates (
            table_name VARCHAR,
            last_run TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            surrogate_key VARCHAR
        );
    """)
