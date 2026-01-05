#!/usr/bin/env python3
"""
Export tables from a local DuckDB file and upload them to MotherDuck (Postgres-compatible)

This script reads every table from the provided DuckDB file into a pandas.DataFrame
and writes it to the target database using SQLAlchemy's `to_sql` (which uses the
Postgres connection string in `MOTHERDUCK_DATABASE_URL`).

Usage:
  export MOTHERDUCK_DATABASE_URL="duckdb:///md:testing" 
  python duckdb_to_motherduck.py --duckdb-file /Users/patyrocks/Documents/pyground/ETL/TMDB.duckdb

Dependencies:
  pip install duckdb pandas sqlalchemy psycopg2-binary
"""

import os
import argparse
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import create_engine


def list_tables(con) -> list:
    # DuckDB: SHOW TABLES returns rows with table names
    try:
        rows = con.execute("SHOW TABLES").fetchall()
        return [r[0] for r in rows]
    except Exception:
        # fallback: query information_schema
        rows = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'")
        return [r[0] for r in rows.fetchall()]


def upload_table(con, engine, table_name, chunksize=10000):
    print(f"Reading table '{table_name}' from DuckDB...")
    df = pd.read_sql_query(f"SELECT * FROM \"{table_name}\"", con)
    print(f"Uploading '{table_name}' ({len(df)} rows) to MotherDuck...")
    df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi', chunksize=chunksize)
    print(f"Uploaded '{table_name}' successfully.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--duckdb-file', '-d', required=True, help='Path to local DuckDB file')
    parser.add_argument('--tables', '-t', nargs='*', help='Optional list of tables to upload (default: all)')
    parser.add_argument('--chunksize', type=int, default=10000, help='Insert chunk size')
    args = parser.parse_args()

    conn_str = os.environ.get('MOTHERDUCK_DATABASE_URL') or os.environ.get('DATABASE_URL')
    if not conn_str:
        print('Error: set MOTHERDUCK_DATABASE_URL environment variable to your MotherDuck Postgres URL')
        return

    duckdb_path = Path(args.duckdb_file).expanduser()
    if not duckdb_path.exists():
        print(f'Error: duckdb file not found: {duckdb_path}')
        return

    print('Connecting to DuckDB...')
    con = duckdb.connect(database=str(duckdb_path), read_only=True)

    print('Connecting to MotherDuck (Postgres)...')
    engine = create_engine(conn_str)

    if args.tables and len(args.tables) > 0:
        tables = args.tables
    else:
        tables = list_tables(con)

    print(f'Found tables: {tables}')

    for t in tables:
        try:
            upload_table(con, engine, t, chunksize=args.chunksize)
        except Exception as e:
            print(f'Failed to upload table {t}: {e}')

    print('Done.')


if __name__ == '__main__':
    main()
