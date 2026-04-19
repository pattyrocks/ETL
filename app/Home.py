import streamlit as st
import streamlit.components.v1 as components
import duckdb
import json
import ast
from collections import Counter
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


def parse_genres(s: str) -> list:
    if not s or s in ("None", "", "[]"):
        return []
    try:
        return ast.literal_eval(s)
    except Exception:
        try:
            return json.loads(s)
        except Exception:
            return []


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

        releases = con.execute("""
            SELECT YEAR(release_date) AS yr, COUNT(*) AS cnt
            FROM movies
            WHERE release_date IS NOT NULL
              AND YEAR(release_date) BETWEEN 1980 AND 2025
            GROUP BY yr
            ORDER BY yr DESC
        """).fetchall()

        genres_raw = con.execute("""
            SELECT genres
            FROM movies
            WHERE genres IS NOT NULL
              AND genres != ''
              AND genres != '[]'
              AND genres != 'None'
            LIMIT 10
        """).fetchall()

        genre_counter: Counter = Counter()
        for (g_str,) in genres_raw:
            for g in parse_genres(g_str):
                if isinstance(g, dict) and g.get("name"):
                    genre_counter[g["name"]] += 1

        decade_ratings = con.execute("""
            SELECT
                (YEAR(release_date) // 10) * 10 AS decade,
                ROUND(AVG(vote_average), 1) AS avg_r
            FROM movies
            WHERE release_date IS NOT NULL
              AND vote_average > 0
              AND vote_count >= 20
              AND YEAR(release_date) BETWEEN 1900 AND 2025
            GROUP BY decade
            ORDER BY decade
        """).fetchall()

        return {
            "total_movies": total_movies,
            "total_tv": total_tv,
            "golden": golden,
            "avg_runtime": int(avg_runtime_row[0]) if avg_runtime_row and avg_runtime_row[0] else 0,
            "releases": releases,
            "top_genres": genre_counter.most_common(4),
            "decade_ratings": decade_ratings,
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


releases = data["releases"]
years = [str(r[0]) for r in releases]
movie_counts = [r[1] for r in releases]
avg_yearly = int(sum(movie_counts) / len(movie_counts)) if movie_counts else 0

peak_idx = movie_counts.index(max(movie_counts)) if movie_counts else 0
peak_year = years[peak_idx] if years else "N/A"

GENRE_COLORS = ["#4A7FD4", "#F76E6E", "#639922", "#c0c0c8"]
top_genres = data["top_genres"]
genre_total = sum(c for _, c in top_genres) or 1
genre_pcts = [(name, round(c / genre_total * 100)) for name, c in top_genres]

decade_ratings = data["decade_ratings"]
max_rating = max((r for _, r in decade_ratings), default=10.0) or 10.0


def decade_bar_color(r: float) -> str:
    ratio = r / max_rating
    if ratio >= 0.9:
        return "#639922"
    if ratio <= 0.75:
        return "#F76E6E"
    return "#4A7FD4"


genre_legend_html = "\n".join(
    f'<div class="leg-item"><div class="leg-sq" style="background:{GENRE_COLORS[i % 4]}"></div>{name} {pct}%</div>'
    for i, (name, pct) in enumerate(genre_pcts)
)

decade_rows_html = "\n".join(
    f"""<div class="genre-row">
          <div class="genre-name">{d}s</div>
          <div class="genre-track"><div class="genre-fill" style="width:{round(r / 10 * 100)}%;background:{decade_bar_color(r)}"></div></div>
          <div class="genre-val">{r}</div>
        </div>"""
    for d, r in decade_ratings
)

today_label = date.today().strftime("%b %Y")
js_counts = json.dumps(movie_counts)
js_years = json.dumps(years)
js_genre_data = json.dumps([pct for _, pct in genre_pcts])
js_genre_labels = json.dumps([name for name, _ in genre_pcts])
js_genre_colors = json.dumps(GENRE_COLORS[: len(genre_pcts)])

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TMDB Analytics</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
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
.brand {{ font-size: 18px; font-weight: 500; color: #1a1a2e; }}
.brand-sub {{ font-size: 11px; color: #9090a8; margin-top: 2px; }}
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
  display: flex; gap: 6px; background: #F5F5FA; border-radius: 20px;
  padding: 6px; border: 0.5px solid rgba(0,0,0,0.06); margin-bottom: 22px; width: fit-content;
}}
.nav-pill {{
  padding: 8px 22px; border-radius: 14px; font-size: 13px; color: #6b6b88;
  cursor: pointer; transition: all 0.18s; border: none; background: none;
  white-space: nowrap; font-family: inherit;
}}
.nav-pill:hover {{ color: #1a1a2e; background: rgba(0,0,0,0.04); }}
.nav-pill.active {{ background: #4A7FD4; color: #fff; font-weight: 500; }}
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
.wave-card {{
  background: #F5F5FA; border-radius: 20px; padding: 24px 28px 20px;
  border: 0.5px solid rgba(0,0,0,0.06); margin-bottom: 18px;
}}
.wave-top {{ display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 4px; }}
.wave-title {{ font-size: 15px; font-weight: 500; color: #1a1a2e; }}
.wave-sub {{ font-size: 11px; color: #9090a8; margin-top: 3px; }}
.chart-wrap {{ position: relative; width: 100%; height: 210px; margin-top: 10px; }}
.tip-bubble {{
  position: absolute; top: 10px; left: 50%; transform: translateX(-50%);
  background: #fff; border-radius: 14px; padding: 7px 16px;
  box-shadow: 0 4px 20px rgba(0,0,0,0.10); text-align: center; pointer-events: none; z-index: 10;
  transition: left 0.08s;
}}
.tip-year {{ font-size: 13px; font-weight: 500; color: #1a1a2e; }}
.tip-label {{ font-size: 11px; color: #9090a8; margin-top: 1px; }}
.wave-bottom {{ display: flex; align-items: flex-end; justify-content: space-between; margin-top: 6px; }}
.wave-legend {{ display: flex; gap: 18px; }}
.leg-item {{ display: flex; align-items: center; gap: 7px; font-size: 12px; color: #6b6b88; }}
.leg-dot {{ width: 10px; height: 10px; border-radius: 50%; }}
.big-stat {{ text-align: right; }}
.big-num {{ font-size: 44px; font-weight: 500; color: #1a1a2e; line-height: 1; letter-spacing: -1px; }}
.big-label {{ font-size: 11px; color: #9090a8; margin-top: 2px; }}
.bottom-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }}
.card {{
  background: #F5F5FA; border-radius: 20px; padding: 22px 24px; border: 0.5px solid rgba(0,0,0,0.06);
}}
.card-title {{ font-size: 14px; font-weight: 500; color: #1a1a2e; margin-bottom: 3px; }}
.card-sub {{ font-size: 11px; color: #9090a8; margin-bottom: 14px; }}
.chart-legend {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 10px; }}
.leg-sq {{ width: 10px; height: 10px; border-radius: 3px; }}
.genre-list {{ display: flex; flex-direction: column; gap: 11px; }}
.genre-row {{ display: flex; align-items: center; gap: 10px; }}
.genre-name {{ font-size: 12px; color: #6b6b88; width: 80px; flex-shrink: 0; }}
.genre-track {{ flex: 1; height: 6px; background: rgba(0,0,0,0.07); border-radius: 3px; overflow: hidden; }}
.genre-fill {{ height: 100%; border-radius: 3px; }}
.genre-val {{ font-size: 11px; font-weight: 500; color: #1a1a2e; width: 32px; text-align: right; }}
</style>
</head>
<body>

<div class="top-header">
  <div>
    <div class="brand">TMDB Analytics</div>
    <div class="brand-sub">Movie &amp; TV Intelligence · {fmt_big(data["total_movies"])} movies · {fmt_big(data["total_tv"])} TV shows</div>
  </div>
  <div class="header-right">
    <div class="date-badge">{today_label}</div>
    <div class="avatar">PN</div>
  </div>
</div>

<div class="top-nav">
  <button class="nav-pill active" onclick="setNav(this)">Overview</button>
  <button class="nav-pill" onclick="setNav(this)">Trends</button>
  <button class="nav-pill" onclick="setNav(this)">Genres</button>
  <button class="nav-pill" onclick="setNav(this)">Ratings</button>
  <button class="nav-pill" onclick="setNav(this)">Decades</button>
</div>

<div class="kpi-row">
  <div class="kpi">
    <div class="kpi-label">Total titles</div>
    <div class="kpi-value">{fmt_big(total)}</div>
    <div class="kpi-change up">↑ {fmt_big(data["total_movies"])} movies + {fmt_big(data["total_tv"])} TV</div>
    <div class="kpi-bar"><div class="kpi-bar-fill" style="width:74%;background:#4A7FD4"></div></div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Golden era</div>
    <div class="kpi-value">{golden_year}</div>
    <div class="kpi-change up">↑ {golden_rating} avg rating</div>
    <div class="kpi-bar"><div class="kpi-bar-fill" style="width:88%;background:#639922"></div></div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Avg runtime 2025</div>
    <div class="kpi-value">{data["avg_runtime"]} min</div>
    <div class="kpi-change up">↑ movies between 30–300 min</div>
    <div class="kpi-bar"><div class="kpi-bar-fill" style="width:60%;background:#F76E6E"></div></div>
  </div>
</div>

<div class="wave-card">
  <div class="wave-top">
    <div>
      <div class="wave-title">Releases over time</div>
      <div class="wave-sub">Movie release trends 1828–2025 · SELECT YEAR(release_date), COUNT(*) FROM movies GROUP BY 1</div>
    </div>
  </div>
  <div class="chart-wrap">
    <div class="tip-bubble" id="tip">
      <div class="tip-year" id="tip-year">{peak_year}</div>
      <div class="tip-label" id="tip-label">Peak releases</div>
    </div>
    <svg id="waveSvg" width="100%" height="100%" viewBox="0 0 800 210" preserveAspectRatio="none" style="display:block;">
      <defs>
        <linearGradient id="rg" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#F76E6E" stop-opacity="0.15"/>
          <stop offset="100%" stop-color="#F76E6E" stop-opacity="0"/>
        </linearGradient>
        <pattern id="dots" x="0" y="0" width="8" height="8" patternUnits="userSpaceOnUse">
          <circle cx="4" cy="4" r="0.8" fill="#c0c0d0" opacity="0.45"/>
        </pattern>
      </defs>
      <rect width="800" height="210" fill="url(#dots)"/>
      <path id="redArea" fill="url(#rg)"/>
      <path id="redLine" fill="none" stroke="#F76E6E" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
      <circle id="hoverDot" r="5" fill="#fff" stroke="#F76E6E" stroke-width="2" opacity="0"/>
    </svg>
  </div>
  <div class="wave-bottom">
    <div class="wave-legend">
      <div class="leg-item"><div class="leg-dot" style="background:#F76E6E"></div>Movies</div>
    </div>
    <div class="big-stat">
      <div class="big-num">{fmt_big(avg_yearly)}</div>
      <div class="big-label">Avg. yearly releases</div>
    </div>
  </div>
</div>

<div class="bottom-row">
  <div class="card">
    <div class="card-title">Genre distribution</div>
    <div class="card-sub">SELECT genre, COUNT(*) FROM movies GROUP BY genre ORDER BY 2 DESC</div>
    <div class="chart-legend">
      {genre_legend_html}
    </div>
    <div style="position:relative;width:100%;height:180px;">
      <canvas id="genreChart"></canvas>
    </div>
  </div>
  <div class="card">
    <div class="card-title">Avg rating by decade</div>
    <div class="card-sub">SELECT decade, AVG(vote_average) FROM movies GROUP BY decade ORDER BY 1</div>
    <div class="genre-list">
      {decade_rows_html}
    </div>
  </div>
</div>

<script>
function setNav(el) {{
  document.querySelectorAll('.nav-pill').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
}}

const moviesData = {js_counts};
const years = {js_years};
const W=800,H=210,PL=20,PR=20,PT=42,PB=16;
const n=moviesData.length;
const mx=Math.max(...moviesData)*1.08;
const xs=years.map((_,i)=>PL+i/(n-1)*(W-PL-PR));
function norm(arr){{return arr.map(v=>PT+(1-v/mx)*(H-PT-PB));}}
const rYs=norm(moviesData);
function smooth(xs,ys){{
  let d=`M ${{xs[0]}} ${{ys[0]}}`;
  for(let i=0;i<xs.length-1;i++){{const cx=(xs[i]+xs[i+1])/2;d+=` C ${{cx}} ${{ys[i]}}, ${{cx}} ${{ys[i+1]}}, ${{xs[i+1]}} ${{ys[i+1]}}`;}}
  return d;
}}
const rP=smooth(xs,rYs);
document.getElementById('redLine').setAttribute('d',rP);
document.getElementById('redArea').setAttribute('d',rP+` L ${{xs[n-1]}} ${{H}} L ${{xs[0]}} ${{H}} Z`);

const pi=moviesData.indexOf(Math.max(...moviesData));
const dot=document.getElementById('hoverDot');
dot.setAttribute('cx',xs[pi]);dot.setAttribute('cy',rYs[pi]);dot.setAttribute('opacity','1');
document.getElementById('tip-year').textContent=years[pi];
document.getElementById('tip-label').textContent=moviesData[pi].toLocaleString()+' movies released';

const svg=document.getElementById('waveSvg');
svg.addEventListener('mousemove',e=>{{
  const r=svg.getBoundingClientRect();
  const mx2=(e.clientX-r.left)*(W/r.width);
  let ci=0,md=Infinity;
  xs.forEach((x,i)=>{{const d=Math.abs(x-mx2);if(d<md){{md=d;ci=i;}}}});
  dot.setAttribute('cx',xs[ci]);dot.setAttribute('cy',rYs[ci]);dot.setAttribute('opacity','1');
  document.getElementById('tip-year').textContent=years[ci];
  document.getElementById('tip-label').textContent=moviesData[ci].toLocaleString()+' movies';
  const tip=document.getElementById('tip');
  tip.style.left=Math.min(Math.max(xs[ci]/W*100,15),85)+'%';
}});
svg.addEventListener('mouseleave',()=>{{
  dot.setAttribute('cx',xs[pi]);dot.setAttribute('cy',rYs[pi]);
  document.getElementById('tip-year').textContent=years[pi];
  document.getElementById('tip-label').textContent=moviesData[pi].toLocaleString()+' movies released';
  document.getElementById('tip').style.left='50%';
}});

new Chart(document.getElementById('genreChart'),{{
  type:'doughnut',
  data:{{
    labels:{js_genre_labels},
    datasets:[{{
      data:{js_genre_data},
      backgroundColor:{js_genre_colors},
      borderWidth:0,
      hoverOffset:4
    }}]
  }},
  options:{{
    responsive:true,
    maintainAspectRatio:false,
    cutout:'68%',
    plugins:{{
      legend:{{display:false}},
      tooltip:{{callbacks:{{label:ctx=>ctx.label+': '+ctx.parsed+'%'}}}}
    }}
  }}
}});
</script>
</body>
</html>"""

components.html(html, height=860, scrolling=True)
