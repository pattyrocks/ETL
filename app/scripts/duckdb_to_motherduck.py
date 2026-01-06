#!/usr/bin/env python3
"""
Export tables from a local DuckDB file and upload them to a MotherDuck (DuckDB) endpoint
or to a DuckDB-compatible endpoint if desired.
"""

import os
import argparse
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import create_engine


def qi(name: str) -> str:
    """Quote an SQL identifier for DuckDB safely."""
    return '"' + str(name).replace('"', '""') + '"'


def list_tables(con) -> list:
    try:
        rows = con.execute("SHOW TABLES").fetchall()
        return [r[0] for r in rows]
    except Exception:
        fallback_rows = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        return [r[0] for r in fallback_rows]


def upload_table(con, engine, table_name, chunksize=10000):
    print(f"Reading table '{table_name}' from DuckDB...")
    # Quote the table identifier to prevent injection via table_name
    df = pd.read_sql_query(f"SELECT * FROM {qi(table_name)}", con)
    print(f"Uploading '{table_name}' ({len(df)} rows) to target...")

    # If engine is a DuckDB connection, write using DuckDB (register DataFrame)
    if isinstance(engine, duckdb.DuckDBPyConnection):
        target_con = engine
        target_con.register("tmp_upload_df", df)
        try:
            target_con.execute(f"CREATE OR REPLACE TABLE {qi(table_name)} AS SELECT * FROM tmp_upload_df;")
            print(f"Uploaded '{table_name}' to DuckDB target successfully.")
        finally:
            try:
                target_con.unregister("tmp_upload_df")
            except Exception:
                pass
    else:
        # Fallback: SQLAlchemy engine (DuckDB-compatible/SQL)
        df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi', chunksize=chunksize)
        print(f"Uploaded '{table_name}' to SQL target successfully.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--duckdb-file', '-d', required=True, help='Path to local DuckDB file')
    parser.add_argument('--tables', '-t', nargs='*', help='Optional list of tables to upload (default: all)')
    parser.add_argument('--chunksize', type=int, default=10000, help='Insert chunk size')
    args = parser.parse_args()

    conn_str = os.environ.get('MOTHERDUCK_DATABASE_URL') or os.environ.get('DATABASE_URL')
    if not conn_str:
        error_msg = (
            "Error: set one of the following environment variables:\n"
            "  - MOTHERDUCK_DATABASE_URL (DuckDB-style, e.g. duckdb://... or md:...)\n"
            "  - DATABASE_URL (DuckDB-style/SQL, e.g. postgres://...)\n"
        )
        print(error_msg)
        return

    duckdb_path = Path(args.duckdb_file).expanduser()
    if not duckdb_path.exists():
        print(f'Error: duckdb file not found: {duckdb_path}')
        return

    print('Connecting to local DuckDB...')
    con = duckdb.connect(database=str(duckdb_path), read_only=True)

    # Decide how to connect to target: prefer DuckDB-style connection for MotherDuck
    target = None
    try:
        if conn_str.startswith("duckdb://") or conn_str.startswith("md:") or conn_str.startswith("duckdb:"):
            print('Connecting to target DuckDB (MotherDuck) endpoint...')
            target = duckdb.connect(database=conn_str)
        elif conn_str.startswith("postgres://") or conn_str.startswith("postgresql://"):
            print('Connecting to DuckDB-compatible target via SQLAlchemy...')
            target = create_engine(conn_str)
        else:
            # Try DuckDB connect first; if fails, try SQLAlchemy
            try:
                target = duckdb.connect(database=conn_str)
            except Exception:
                target = create_engine(conn_str)
    except Exception as e:
        print(f'Error connecting to target: {e}')
        return

    if args.tables and len(args.tables) > 0:
        tables = args.tables
    else:
        tables = list_tables(con)

    print(f'Found tables: {tables}')

    for t in tables:
        try:
            upload_table(con, target, t, chunksize=args.chunksize)
        except Exception as e:
            print(f'Failed to upload table {t}: {e}')

    print('Done.')


if __name__ == '__main__':
    main()
