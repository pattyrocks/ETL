# App

Next.js app scaffold for TMDB data backed by MotherDuck.

Quick start (local):

1. Install dependencies

```bash
cd App
npm install
```

2. Set environment variable for MotherDuck (Postgres-compatible) connection string:

```bash
export MOTHERDUCK_DATABASE_URL="duckdb:///md:TMDB"
```

3. Run dev server

```bash
npm run dev
```

The example API route `/api/movies` will query the `movies` table in MotherDuck.

Deployment: this project is intended for deployment to Vercel. Add `MOTHERDUCK_DATABASE_URL` to Vercel Environment Variables.

Next steps:
- Wire ETL to ingest TMDB data into MotherDuck tables.
- Build UI pages (browse, detail, search) and pagination.

Upload DuckDB to MotherDuck (local script)

1. Install Python dependencies:

```bash
python -m pip install duckdb pandas sqlalchemy psycopg2-binary
```

2. Run the included script to upload all tables from a DuckDB file to MotherDuck:

```bash
export MOTHERDUCK_DATABASE_URL="duckdb:///md:TMDB"
python App/scripts/duckdb_to_motherduck.py --duckdb-file /Users/patyrocks/Documents/pyground/ETL/TMDB
```

The script will create or replace tables in MotherDuck with the same table names and basic types inferred by pandas/SQLAlchemy.

