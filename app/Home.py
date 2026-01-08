import streamlit as st

st.set_page_config(
    page_title="TMDB Dashboard",
    page_icon="🎬",
    layout="centered"
)

st.title("🎬 TMDB Dashboard")
st.markdown("---")

st.markdown("""
Welcome to the TMDB Dashboard.

Use the sidebar to navigate:
- **Top 10 Movies** - See the most popular movies of 2025
""")

st.markdown("---")
st.caption("Data sourced from TMDB API")

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: left; color: grey;'>see how I built this on <a href='https://github.com/pattyrocks/ETL' target='_blank'>GitHub</a> 👩🏽‍💻</div>",
    unsafe_allow_html=True
)