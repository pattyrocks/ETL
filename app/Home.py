import streamlit as st
import streamlit.components.v1 as components
import duckdb
import json
import pycountry
from collections import defaultdict
from datetime import date

st.set_page_config(
    page_title="TMDB Analytics",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """<style>
    #MainMenu, header, footer { visibility: hidden; }
    .block-container { padding: 0 !important; max-width: 100% !important; }
    </style>""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=3600, show_spinner="Querying MotherDuck…")
def load_data(token: str) -> dict:
    con = duckdb.connect(f"md:TMDB?motherduck_token={token}", read_only=True)
    try:
        total_movies = con.execute(
            "SELECT COUNT(*) FROM movies WHERE title IS NOT NULL"
        ).fetchone()[0]

        total_tv = con.execute(
            "SELECT COUNT(*) FROM tv_shows WHERE name IS NOT NULL"
        ).fetchone()[0]

        golden = con.execute("""
            SELECT YEAR(release_date) AS yr, ROUND(AVG(vote_average), 1) AS avg_r
            FROM movies
            WHERE release_date IS NOT NULL
              AND vote_count >= 50
              AND vote_average > 0
            GROUP BY yr
            HAVING COUNT(*) >= 10
            ORDER BY avg_r DESC
            LIMIT 1
        """).fetchone()

        avg_runtime_row = con.execute("""
            SELECT ROUND(AVG(runtime))
            FROM movies
            WHERE YEAR(release_date) = 2025
        """).fetchone()

        top5_all = con.execute("""
            SELECT 
              title,
              UNNEST(origin_country) AS country_code,
              EXTRACT(YEAR FROM release_date) as year,
              rank() over (partition by year order by popularity DESC) as rank
            FROM movies
            WHERE EXTRACT(YEAR FROM release_date) between EXTRACT(YEAR FROM current_date()) - 9 and EXTRACT(YEAR FROM current_date()) - 1
            QUALIFY rank <= 5
            ORDER BY year DESC, rank
        """).fetchall()

        return {
            "total_movies": total_movies,
            "total_tv": total_tv,
            "golden": golden,
            "avg_runtime": int(avg_runtime_row[0]) if avg_runtime_row and avg_runtime_row[0] else 0,
            "top5_all": top5_all,
        }
    finally:
        con.close()


token = st.secrets["MOTHERDUCK_TOKEN"]
data = load_data(token)

# --- derived values ---
total = data["total_movies"] + data["total_tv"]
golden_year = data["golden"][0] if data["golden"] else "N/A"
golden_rating = data["golden"][1] if data["golden"] else 0.0


def fmt_big(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return str(n)


def country_name(code: str) -> str:
    if not code:
        return 'Unknown'
    c = pycountry.countries.get(alpha_2=code)
    return c.name if c else code


# Group top5 by year — columns: title, country_code, year, rank
top5_by_year = defaultdict(list)
for title, country_code, yr, rank in data["top5_all"]:
    top5_by_year[int(yr)].append((title, country_name(country_code), rank))

sorted_years = sorted(top5_by_year.keys(), reverse=True)

year_cards_html = ""
for yr in sorted_years:
    movies = top5_by_year[yr]
    rows = "\n".join(
        f'<div class="top5-row">'
        f'<div class="top5-rank">{rank}</div>'
        f'<div class="top5-info"><div class="top5-title">{title}</div>'
        f'<div class="top5-country">{country}</div></div>'
        f'<div class="top5-bar-wrap"><div class="top5-bar" style="width:{round((6-rank)/5*100)}%"></div></div>'
        f'</div>'
        for title, country, rank in movies
    )
    year_cards_html += (
        f'<div class="card year-card">'
        f'<div class="card-title">{yr}</div>'
        f'<div class="top5-list">{rows}</div></div>'
    )

today_label = date.today().strftime("%b %Y")

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TMDB Analytics</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  background: #EEEEF3;
  min-height: 100vh;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  padding: 28px 28px 36px;
}}
.top-header {{
  display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px;
}}
.brand {{ font-size: 26px; font-weight: 600; color: #1a1a2e; }}
.brand-sub {{ font-size: 14px; color: #9090a8; margin-top: 3px; }}
.header-right {{ display: flex; align-items: center; gap: 10px; }}
.date-badge {{
  font-size: 12px; color: #6b6b88; background: #F5F5FA; border-radius: 20px;
  padding: 7px 16px; border: 0.5px solid rgba(0,0,0,0.07);
}}
.avatar {{
  width: 34px; height: 34px; border-radius: 50%; background: #d8e4f8; color: #4A7FD4;
  font-size: 12px; font-weight: 500; display: flex; align-items: center; justify-content: center;
}}
.top-nav {{
  display: flex; gap: 10px; margin-bottom: 22px; width: fit-content;
}}
.nav-pill {{
  padding: 10px 28px; border-radius: 24px; font-size: 14px; color: #1a1a2e;
  font-weight: 500; cursor: pointer; transition: all 0.18s;
  border: 1.5px solid rgba(0,0,0,0.12); background: #fff;
  white-space: nowrap; font-family: inherit;
}}
.nav-pill:hover {{ color: #1a1a2e; background: rgba(0,0,0,0.04); }}
.nav-pill.active {{ background: #fff; color: #1a1a2e; font-weight: 500; border-color: rgba(0,0,0,0.25); }}
.kpi-row {{ display: grid; grid-template-columns: repeat(3,1fr); gap: 14px; margin-bottom: 18px; }}
.kpi {{
  background: #F5F5FA; border-radius: 18px; padding: 18px 20px; border: 0.5px solid rgba(0,0,0,0.06);
}}
.kpi-label {{ font-size: 12px; color: #9090a8; margin-bottom: 6px; }}
.kpi-value {{ font-size: 26px; font-weight: 500; color: #1a1a2e; line-height: 1; }}
.kpi-change {{ font-size: 11px; margin-top: 6px; }}
.kpi-change.up {{ color: #3B6D11; }}
.kpi-bar {{ height: 3px; background: rgba(0,0,0,0.07); border-radius: 2px; margin-top: 10px; overflow: hidden; }}
.kpi-bar-fill {{ height: 100%; border-radius: 2px; }}
.section-header {{ margin-bottom: 14px; }}
.section-title {{ font-size: 22px; font-weight: 600; color: #1a1a2e; }}
.section-sub {{ font-size: 13px; color: #9090a8; margin-top: 4px; }}
.scroll-container {{
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
  padding-bottom: 10px;
  scrollbar-width: thin;
  scrollbar-color: rgba(0,0,0,0.15) transparent;
}}
.scroll-container::-webkit-scrollbar {{ height: 6px; }}
.scroll-container::-webkit-scrollbar-track {{ background: transparent; }}
.scroll-container::-webkit-scrollbar-thumb {{ background: rgba(0,0,0,0.15); border-radius: 3px; }}
.scroll-track {{
  display: flex;
  gap: 14px;
}}
.card {{
  background: #F5F5FA; border-radius: 20px; padding: 22px 24px; border: 0.5px solid rgba(0,0,0,0.06);
}}
.year-card {{
  min-width: calc(25% - 11px);
  max-height: 420px;
  overflow-y: auto;
  flex-shrink: 0;
  scrollbar-width: thin;
  scrollbar-color: rgba(0,0,0,0.12) transparent;
}}
.card-title {{ font-size: 14px; font-weight: 500; color: #1a1a2e; margin-bottom: 3px; }}
.card-sub {{ font-size: 11px; color: #9090a8; margin-bottom: 14px; font-family: 'SF Mono', SFMono-Regular, Menlo, monospace; letter-spacing: 0.3px; }}
.top5-list {{ display: flex; flex-direction: column; gap: 20px; }}
.top5-row {{ display: flex; align-items: center; gap: 14px; }}
.top5-rank {{ font-size: 28px; font-weight: 300; color: #c0c0c8; width: 32px; text-align: center; flex-shrink: 0; }}
.top5-info {{ width: 200px; flex-shrink: 0; }}
.top5-title {{ font-size: 15px; font-weight: 600; color: #1a1a2e; line-height: 1.3; }}
.top5-country {{ font-size: 12px; color: #9090a8; margin-top: 2px; }}
.top5-bar-wrap {{ flex: 1; height: 8px; background: rgba(0,0,0,0.06); border-radius: 4px; overflow: hidden; }}
.top5-bar {{ height: 100%; border-radius: 4px; background: rgba(74,127,212,0.55); }}
.top5-score {{ font-size: 14px; font-weight: 500; color: #6b6b88; width: 44px; text-align: right; flex-shrink: 0; }}
</style>
</head>
<body>

<div class="top-header">
  <div>
    <div class="brand">TMDB Analytics</div>
    <div class="brand-sub">Movie &amp; TV Intelligence &middot; {fmt_big(total)} rows &middot; {today_label}</div>
  </div>
</div>

<div class="top-nav">
  <button class="nav-pill active" onclick="setNav(this)">Overview</button>
  <button class="nav-pill" onclick="setNav(this)">Titles</button>
  <button class="nav-pill" onclick="setNav(this)">Cast</button>
  <button class="nav-pill" onclick="setNav(this)">Production</button>
</div>

<div class="kpi-row">
  <div class="kpi">
    <div class="kpi-label">Total titles</div>
    <div class="kpi-value">{fmt_big(total)}</div>
    <div class="kpi-change up">&#8593; {fmt_big(data["total_movies"])} movies + {fmt_big(data["total_tv"])} TV</div>
    <div class="kpi-bar"><div class="kpi-bar-fill" style="width:74%;background:#4A7FD4"></div></div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Golden era</div>
    <div class="kpi-value">{golden_year}</div>
    <div class="kpi-change up">&#8593; {golden_rating} avg rating</div>
    <div class="kpi-bar"><div class="kpi-bar-fill" style="width:88%;background:#639922"></div></div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Avg runtime 2025</div>
    <div class="kpi-value">{data["avg_runtime"]} min</div>
    <div class="kpi-change up">&#8593; movies between 30&#8211;300 min</div>
    <div class="kpi-bar"><div class="kpi-bar-fill" style="width:60%;background:#F76E6E"></div></div>
  </div>
</div>

<div class="section-header">
  <div class="section-title">Last 10 years Top 5</div>
  <div class="section-sub">Ranked by TMDB popularity score</div>
</div>
<div class="scroll-container">
  <div class="scroll-track">
    {year_cards_html}
  </div>
</div>

<script>
function setNav(el) {{
  document.querySelectorAll('.nav-pill').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
}}
</script>
</body>
</html>"""

components.html(html, height=620, scrolling=True)
