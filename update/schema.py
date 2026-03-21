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
            overview VARCHAR,
            poster_path VARCHAR,
            backdrop_path VARCHAR,
            popularity DOUBLE,
            vote_average DOUBLE,
            vote_count INTEGER,
            first_air_date DATE,
            last_air_date DATE,
            episode_run_time VARCHAR,
            homepage VARCHAR,
            in_production BOOLEAN,
            number_of_episodes INTEGER,
            number_of_seasons INTEGER,
            origin_country VARCHAR,
            original_language VARCHAR,
            original_name VARCHAR,
            production_countries VARCHAR,
            genres VARCHAR,
            networks VARCHAR,
            created_by VARCHAR,
            status VARCHAR,
            type VARCHAR,
            tagline VARCHAR,
            adult BOOLEAN,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def ensure_cast_crew_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS tv_show_cast_crew (
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
            also_known_as VARCHAR,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS movie_crew (
            movie_id BIGINT,
            person_id BIGINT,
            name VARCHAR,
            credit_id VARCHAR,
            gender INTEGER,
            profile_path VARCHAR,
            known_for_department VARCHAR,
            popularity DOUBLE,
            original_name VARCHAR,
            adult BOOLEAN,
            department VARCHAR,
            job VARCHAR,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
