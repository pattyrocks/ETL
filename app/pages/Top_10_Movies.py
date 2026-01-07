import streamlit as st
import duckdb
import os

st.set_page_config(
    page_title="Top 10 Movies 2025",
    page_icon="🎬",
    layout="centered"
)

st.title("🏆 Top 10 Movies of 2025")
st.markdown("---")

# Get MotherDuck token from Streamlit secrets or environment
try:
    motherduck_token = st.secrets["MOTHERDUCK_TOKEN"]
except:
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")

try:
    conn = duckdb.connect(f"md:TMDB?motherduck_token={motherduck_token}")
    
    query = """
        SELECT 
            title,
            release_date,
            popularity,
            vote_count,
            vote_average
        FROM movies
        WHERE release_date >= '2025-01-01'
        ORDER BY popularity DESC
        LIMIT 10
    """
    
    df = conn.execute(query).fetchdf()
    conn.close()
    
    if df.empty:
        st.warning("No movies found for 2025 yet.")
    else:
        # Display as a clean table
        st.dataframe(
            df,
            column_config={
                "title": "Title",
                "release_date": "Release Date",
                "popularity": st.column_config.NumberColumn("Popularity", format="%.1f"),
                "vote_count": st.column_config.NumberColumn("Votes", format="%d"),
                "vote_average": st.column_config.NumberColumn("Rating", format="%.1f ⭐")
            },
            hide_index=True,
            use_container_width=True
        )

except Exception as e:
    st.error(f"Could not connect to MotherDuck: {e}")