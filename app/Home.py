import streamlit as st
import streamlit.components.v1 as components
import duckdb
import json
import pycountry
from collections import defaultdict
from datetime import date
import math

st.set_page_config(
    page_title="TMDB Analytics",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """<style>
    #MainMenu, header, footer { visibility: hidden; }
    .block-container { padding: 0 !important; max-width: 100% !important; }
    .stApp > header { display: none; }
    .stMainBlockContainer { padding-top: 0 !important; padding-bottom: 0 !important; }
    iframe { border: none !important; }
    section[data-testid="stSidebar"] { display: none; }
    div[data-testid="stAppViewBlockContainer"] { padding: 0 !important; }
    </style>""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=300, show_spinner="Querying MotherDuck…")
def load_data(token: str) -> dict:
    con = duckdb.connect(f"md:TMDB?motherduck_token={token}", read_only=True)
    try:
        kpi_row = con.execute("""
            SELECT 
                (SELECT title FROM movies WHERE EXTRACT(YEAR FROM release_date) <= EXTRACT(YEAR FROM current_date()) AND adult = FALSE ORDER BY release_date DESC, id DESC LIMIT 1) AS last_released_movie_title,
                (SELECT id FROM movies WHERE EXTRACT(YEAR FROM release_date) <= EXTRACT(YEAR FROM current_date()) AND adult = FALSE ORDER BY release_date DESC, id DESC LIMIT 1) AS last_released_movie_id,
                (SELECT origin_country[1] FROM movies WHERE EXTRACT(YEAR FROM release_date) <= EXTRACT(YEAR FROM current_date()) AND adult = FALSE ORDER BY release_date DESC, id DESC LIMIT 1) AS last_released_movie_country,
                (SELECT COUNT(*) FROM movies WHERE release_date >= CURRENT_DATE - INTERVAL '1 day') AS total_movies_released_since_yesterday,
                (SELECT COUNT(*) FROM movies) AS total_movies_ever_released,
                (SELECT year FROM (
                    SELECT EXTRACT(YEAR FROM release_date) AS year,
                           COUNT(*) AS count
                    FROM movies 
                    WHERE release_date is not null
                      AND adult = FALSE
                    GROUP BY year 
                    ORDER BY count DESC 
                    LIMIT 1
                )
                ) AS year_with_highest_number_of_movie_releases
        """).fetchone()

        top5_all = con.execute("""
            SELECT 
              title,
              origin_country[1] AS country_code,
              EXTRACT(YEAR FROM release_date) as year,
              ROW_NUMBER() over (partition by year order by popularity DESC) as rank,
              id
            FROM movies
            WHERE id <> 1040159 AND adult = FALSE AND EXTRACT(YEAR FROM release_date) between EXTRACT(YEAR FROM current_date()) - 10 and EXTRACT(YEAR FROM current_date()) - 1
            QUALIFY rank <= 5
            ORDER BY year DESC, rank
        """).fetchall()

        heatmap_raw = con.execute("""
            select
              popularity, vote_average, budget, revenue
            from movies
            where budget > 0 and revenue > 0 and popularity > 0 and vote_average > 0 and vote_count >= 100
            and adult = false and EXTRACT(YEAR FROM release_date) <= EXTRACT(YEAR FROM current_date())
            and budget between
              (select percentile_cont(0.01) within group (order by budget) from movies where budget > 0 and adult = false)
              and (select percentile_cont(0.99) within group (order by budget) from movies where budget > 0 and adult = false)
            and revenue between
              (select percentile_cont(0.01) within group (order by revenue) from movies where revenue > 0 and adult = false)
              and (select percentile_cont(0.99) within group (order by revenue) from movies where revenue > 0 and adult = false)
            and popularity between
              (select percentile_cont(0.01) within group (order by popularity) from movies where popularity > 0 and adult = false)
              and (select percentile_cont(0.99) within group (order by popularity) from movies where popularity > 0 and adult = false)
            and vote_average between
              (select percentile_cont(0.01) within group (order by vote_average) from movies where vote_average > 0 and adult = false)
              and (select percentile_cont(0.99) within group (order by vote_average) from movies where vote_average > 0 and adult = false)
            and vote_count between
              (select percentile_cont(0.01) within group (order by vote_count) from movies where vote_count > 0 and adult = false)
              and (select percentile_cont(0.99) within group (order by vote_count) from movies where vote_count > 0 and adult = false)
        """).fetchall()

        return {
            "last_title": kpi_row[0],
            "last_movie_id": kpi_row[1],
            "last_country": kpi_row[2],
            "released_since_yesterday": kpi_row[3],
            "total_movies": kpi_row[4],
            "peak_year": int(kpi_row[5]) if kpi_row[5] else "N/A",
            "top5_all": top5_all,
            "heatmap": heatmap_raw,
        }
    finally:
        con.close()


token = st.secrets["MOTHERDUCK_TOKEN"]
data = load_data(token)


def fmt_big(n: int) -> str:
    return f"{n:,}"


def fmt_short(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.2f}K"
    return str(n)


def country_name(code: str) -> str:
    if not code:
        return ''
    c = pycountry.countries.get(alpha_2=code)
    return c.name if c else code


# --- derived values ---
last_title = data["last_title"] or "N/A"
last_movie_id = data["last_movie_id"]
last_country_code = data["last_country"] or ""
last_country = country_name(last_country_code)
released_yesterday = data["released_since_yesterday"] or 0
total_movies = data["total_movies"] or 0
peak_year = data["peak_year"]


# Group top5 by year — columns: title, country_code, year, rank, id
top5_by_year = defaultdict(list)
for title, country_code, yr, rank, movie_id in data["top5_all"]:
    top5_by_year[int(yr)].append((title, country_name(country_code), rank, movie_id))

sorted_years = sorted(top5_by_year.keys(), reverse=True)

year_cards_html = ""
for yr in sorted_years:
    movies = top5_by_year[yr]
    rows = ""
    for title, country, rank, movie_id in movies:
        country_div = f'<div class="top5-country">{country}</div>' if country else ''
        rows += (
            f'<div class="top5-row">'
            f'<div class="top5-rank">{rank}</div>'
            f'<div class="top5-info"><div class="top5-title"><a href="https://www.themoviedb.org/movie/{movie_id}" target="_blank" rel="noopener">{title}</a></div>'
            f'{country_div}</div>'
            f'</div>'
        )
    year_cards_html += (
        f'<div class="card year-card">'
        f'<div class="card-title">{yr}</div>'
        f'<div class="top5-list">{rows}</div></div>'
    )

# --- heatmap binning (quantile-based, shared sample) ---
pops = sorted([p for p, v, b, r in data["heatmap"]])
n_bins = 8
quantile_edges = [pops[int(len(pops) * i / n_bins)] for i in range(n_bins)] + [float('inf')]

def fmt_edge(v):
    return f'{v:.0f}' if v >= 10 else f'{v:.1f}'

pop_labels = [f'{fmt_edge(quantile_edges[i])}-{fmt_edge(quantile_edges[i+1])}' if quantile_edges[i+1] != float('inf') else f'{fmt_edge(quantile_edges[i])}+' for i in range(n_bins)]

votes = sorted([v for p, v, b, r in data["heatmap"]])
vote_edges = [votes[int(len(votes) * i / n_bins)] for i in range(n_bins)] + [float('inf')]
vote_labels = [f'{vote_edges[i]:.1f}-{vote_edges[i+1]:.1f}' if vote_edges[i+1] != float('inf') else f'{vote_edges[i]:.1f}+' for i in range(n_bins)]

hm_acc = {}
for pop, vote, budget, revenue in data["heatmap"]:
    pi = next((i for i in range(len(quantile_edges) - 1) if quantile_edges[i] <= pop < quantile_edges[i + 1]), n_bins - 1)
    vi = next((i for i in range(len(vote_edges) - 1) if vote_edges[i] <= vote < vote_edges[i + 1]), n_bins - 1)
    key = (vi, pi)
    if key not in hm_acc:
        hm_acc[key] = [0, 0]
    hm_acc[key][0] += math.log10(budget) if budget > 0 else 0
    hm_acc[key][1] += 1

heatmap_z = []
for vi in range(len(vote_labels)):
    row = []
    for pi in range(len(pop_labels)):
        s = hm_acc.get((vi, pi))
        row.append(round(s[0] / s[1], 2) if s else None)
    heatmap_z.append(row)

heatmap_json = json.dumps({"z": heatmap_z, "x": pop_labels, "y": vote_labels})

# --- heatmap 2: vote_average as color, budget on Y, popularity on X ---
budgets = sorted([b for p, v, b, r in data["heatmap"]])
budget_edges = [budgets[int(len(budgets) * i / n_bins)] for i in range(n_bins)] + [float('inf')]

def fmt_budget_edge(v):
    if v >= 1_000_000:
        return f'${v/1_000_000:.0f}M'
    if v >= 1_000:
        return f'${v/1_000:.0f}K'
    return f'${v:.0f}'

budget_labels = [f'{fmt_budget_edge(budget_edges[i])}-{fmt_budget_edge(budget_edges[i+1])}' if budget_edges[i+1] != float('inf') else f'{fmt_budget_edge(budget_edges[i])}+' for i in range(n_bins)]

hm2_acc = {}
for pop, vote, budget, revenue in data["heatmap"]:
    pi = next((i for i in range(len(quantile_edges) - 1) if quantile_edges[i] <= pop < quantile_edges[i + 1]), n_bins - 1)
    bi = next((i for i in range(len(budget_edges) - 1) if budget_edges[i] <= budget < budget_edges[i + 1]), n_bins - 1)
    key = (bi, pi)
    if key not in hm2_acc:
        hm2_acc[key] = [0, 0]
    hm2_acc[key][0] += vote
    hm2_acc[key][1] += 1

heatmap2_z = []
for bi in range(len(budget_labels)):
    row = []
    for pi in range(len(pop_labels)):
        s = hm2_acc.get((bi, pi))
        row.append(round(s[0] / s[1], 1) if s else None)
    heatmap2_z.append(row)

heatmap2_json = json.dumps({"z": heatmap2_z, "x": pop_labels, "y": budget_labels})

# --- heatmap 3: revenue heatmap (quantile vote_average Y, popularity X, log10(revenue) color) ---
hm3_acc = {}
for pop, vote, budget, revenue in data["heatmap"]:
    pi = next((i for i in range(len(quantile_edges) - 1) if quantile_edges[i] <= pop < quantile_edges[i + 1]), n_bins - 1)
    vi = next((i for i in range(len(vote_edges) - 1) if vote_edges[i] <= vote < vote_edges[i + 1]), n_bins - 1)
    key = (vi, pi)
    if key not in hm3_acc:
        hm3_acc[key] = [0, 0]
    hm3_acc[key][0] += math.log10(revenue) if revenue > 0 else 0
    hm3_acc[key][1] += 1

heatmap3_z = []
for vi in range(len(vote_labels)):
    row = []
    for pi in range(len(pop_labels)):
        s = hm3_acc.get((vi, pi))
        row.append(round(s[0] / s[1], 2) if s else None)
    heatmap3_z.append(row)

heatmap3_json = json.dumps({"z": heatmap3_z, "x": pop_labels, "y": vote_labels})
hm3_flat = [v for row in heatmap3_z for v in row if v is not None]
hm3_zmin = min(hm3_flat) if hm3_flat else 4
hm3_zmax = max(hm3_flat) if hm3_flat else 9

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
.kpi-value a {{ color: #1a1a2e; text-decoration: none; }}
.kpi-value a:hover {{ color: #4A7FD4; }}
.kpi-change {{ font-size: 11px; margin-top: 6px; }}
.kpi-change.up {{ color: #4A7FD4; }}
.kpi-bar {{ display: none; }}
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
  min-width: 260px;
  width: calc(25% - 11px);
  height: 420px;
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
  overflow: hidden;
}}
.year-card .card-title {{
  flex-shrink: 0;
  margin-bottom: 14px;
}}
.year-card .top5-list {{
  flex: 1;
  overflow-y: auto;
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
.top5-title a {{ color: #1a1a2e; text-decoration: none; }}
.top5-title a:hover {{ color: #4A7FD4; }}
.top5-country {{ font-size: 12px; color: #9090a8; margin-top: 2px; }}
.top5-bar-wrap {{ flex: 1; height: 8px; background: rgba(0,0,0,0.06); border-radius: 4px; overflow: hidden; }}
.top5-bar {{ height: 100%; border-radius: 4px; background: rgba(74,127,212,0.55); }}
.top5-score {{ font-size: 14px; font-weight: 500; color: #6b6b88; width: 44px; text-align: right; flex-shrink: 0; }}
.footnote {{ text-align: center; margin-top: 28px; font-size: 12px; color: #9090a8; }}
.footnote a {{ color: #4A7FD4; text-decoration: none; font-weight: 500; }}
.footnote a:hover {{ text-decoration: underline; }}
</style>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body>

<div class="top-header">
  <div>
    <div class="brand">TMDB Analytics</div>
    <div class="brand-sub">Movies &amp; TV &middot; {fmt_big(total_movies)} movies &middot; {today_label}</div>
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
    <div class="kpi-label">Last released movie</div>
    <div class="kpi-value" style="font-size:18px"><a href="https://www.themoviedb.org/movie/{last_movie_id}" target="_blank" rel="noopener">{last_title}</a></div>
    <div class="kpi-change up">{last_country}</div>
    <div class="kpi-bar"><div class="kpi-bar-fill" style="width:100%;background:#4A7FD4"></div></div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Released movies since yesterday</div>
    <div class="kpi-value">{fmt_big(released_yesterday)}</div>
    <div class="kpi-change up">of {fmt_short(total_movies)} total movies</div>
    <div class="kpi-bar"><div class="kpi-bar-fill" style="width:60%;background:#639922"></div></div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Biggest year ever</div>
    <div class="kpi-value">{peak_year}</div>
    <div class="kpi-change up">Most movie releases</div>
    <div class="kpi-bar"><div class="kpi-bar-fill" style="width:88%;background:#F76E6E"></div></div>
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

<div class="section-header" style="margin-top:24px">
  <div class="section-title">Budget is directly associated with popularity, but not to vote average</div>
  <div class="section-sub">Popularity increases with budget, but high budget movies tend to have higher mid-range vote averages</div>
</div>
<div style="background:#F5F5FA;border-radius:20px;padding:22px 24px;border:0.5px solid rgba(0,0,0,0.06)">
  <div id="heatmap" style="width:100%;height:400px"></div>
</div>

<div class="section-header" style="margin-top:24px">
  <div class="section-title">Vote average is higher for low budget movies, but increases with popularity</div>
</div>
<div style="background:#F5F5FA;border-radius:20px;padding:22px 24px;border:0.5px solid rgba(0,0,0,0.06)">
  <div id="heatmap2" style="width:100%;height:400px"></div>
</div>

<div class="section-header" style="margin-top:24px">
  <div class="section-title">Revenue grows with popularity, but vote average shows weak relation to revenue</div>
  <div class="section-sub">Revenue is also high for high vote average movies, but so is the case for low vote average movies</div>
</div>
<div style="background:#F5F5FA;border-radius:20px;padding:22px 24px;border:0.5px solid rgba(0,0,0,0.06)">
  <div id="heatmap3" style="width:100%;height:400px"></div>
</div>

<div class="footnote">
  Made by <a href="https://www.linkedin.com/in/patricians" target="_blank" rel="noopener">pattyrocks</a> &#x1F469;&#x1F3FD;&#x200D;&#x1F4BB;
</div>

<script>
function setNav(el) {{
  document.querySelectorAll('.nav-pill').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
}}

var hdata = {heatmap_json};
Plotly.newPlot('heatmap', [{{
  z: hdata.z,
  x: hdata.x,
  y: hdata.y,
  type: 'heatmap',
  colorscale: [[0, '#EEEEF3'], [0.5, '#4A7FD4'], [1, '#1a1a2e']],
  hoverongaps: false,
  hovertemplate: 'Popularity: %{{x}}<br>Vote Avg: %{{y}}<br>Avg Budget: $%{{z}}M (log\u2081\u2080)<extra></extra>',
  colorbar: {{
    title: {{text: 'Budget', font: {{size: 12}}}},
    tickvals: [4, 5, 6, 7, 8],
    ticktext: ['$10K', '$100K', '$1M', '$10M', '$100M'],
    thickness: 12,
    len: 0.8
  }}
}}], {{
  xaxis: {{title: {{text: 'Popularity', font: {{size: 13}}}}, tickfont: {{size: 11}}}},
  yaxis: {{title: {{text: 'Vote Average', font: {{size: 13}}}}, tickfont: {{size: 11}}}},
  paper_bgcolor: 'transparent',
  plot_bgcolor: 'transparent',
  margin: {{t: 10, b: 50, l: 60, r: 80}},
  font: {{family: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif", color: '#1a1a2e'}}
}}, {{responsive: true, displayModeBar: false}});

var hdata2 = {heatmap2_json};
Plotly.newPlot('heatmap2', [{{
  z: hdata2.z,
  x: hdata2.x,
  y: hdata2.y,
  type: 'heatmap',
  colorscale: [[0, '#EEEEF3'], [0.5, '#e8a838'], [1, '#c0392b']],
  hoverongaps: false,
  hovertemplate: 'Popularity: %{{x}}<br>Budget: %{{y}}<br>Avg Vote: %{{z:.1f}}<extra></extra>',
  colorbar: {{
    title: {{text: 'Avg Vote', font: {{size: 12}}}},
    thickness: 12,
    len: 0.8
  }}
}}], {{
  xaxis: {{title: {{text: 'Popularity', font: {{size: 13}}}}, tickfont: {{size: 11}}}},
  yaxis: {{title: {{text: 'Budget', font: {{size: 13}}}}, tickfont: {{size: 11}}}},
  paper_bgcolor: 'transparent',
  plot_bgcolor: 'transparent',
  margin: {{t: 10, b: 50, l: 100, r: 80}},
  font: {{family: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif", color: '#1a1a2e'}}
}}, {{responsive: true, displayModeBar: false}});

var hdata3 = {heatmap3_json};
var zmin3 = {hm3_zmin};
var zmax3 = {hm3_zmax};
Plotly.newPlot('heatmap3', [{{
  z: hdata3.z,
  x: hdata3.x,
  y: hdata3.y,
  type: 'heatmap',
  zmin: zmin3,
  zmax: zmax3,
  colorscale: [[0, '#e8f5e9'], [0.25, '#81c784'], [0.5, '#27ae60'], [0.75, '#1b7a3d'], [1, '#0d3318']],
  hoverongaps: false,
  hovertemplate: 'Popularity: %{{x}}<br>Vote Avg: %{{y}}<br>Avg Revenue: $%{{z}}M (log\u2081\u2080)<extra></extra>',
  colorbar: {{
    title: {{text: 'Revenue', font: {{size: 12}}}},
    thickness: 12,
    len: 0.8
  }}
}}], {{
  xaxis: {{title: {{text: 'Popularity', font: {{size: 13}}}}, tickfont: {{size: 11}}}},
  yaxis: {{title: {{text: 'Vote Average', font: {{size: 13}}}}, tickfont: {{size: 11}}}},
  paper_bgcolor: 'transparent',
  plot_bgcolor: 'transparent',
  margin: {{t: 10, b: 50, l: 60, r: 80}},
  font: {{family: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif", color: '#1a1a2e'}}
}}, {{responsive: true, displayModeBar: false}});
</script>
</body>
</html>"""

components.html(html, height=2400, scrolling=False)
