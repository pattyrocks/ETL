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
export MOTHERDUCK_DATABASE_URL="postgres://user:pass@host:5432/dbname"
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
