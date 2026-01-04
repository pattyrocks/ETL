"""
Scan DuckDB and suggest migrations for integer-like columns that may overflow 32-bit INT.
- Fetches MIN/MAX per column; if MIN or MAX is NULL it fetches the first non-null value.
- Detects INT columns (excluding BIGINT) and DOUBLE/REAL/FLOAT columns with integer values out of INT32 range.
- Can emit safe CREATE TABLE ... AS SELECT ... SQL to cast affected columns (including 'id') to BIGINT.
Usage:
  python scan_integer_columns.py --db TMDB
  python scan_integer_columns.py --db TMDB --generate-sql > suggested_migrations.sql
"""
import argparse
import duckdb
import sys
import pandas as pd

INT32_MIN = -2_147_483_648
INT32_MAX = 2_147_483_647

def qi(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'

def main():
    p = argparse.ArgumentParser(description="Scan DuckDB for int columns that may need BIGINT.")
    p.add_argument("--db", default="TMDB", help="DuckDB file path (default: TMDB)")
    p.add_argument("--generate-sql", action="store_true", help="Emit safe CREATE TABLE ... AS SELECT ... SQL to cast affected columns to BIGINT")
    args = p.parse_args()

    con = duckdb.connect(database=args.db, read_only=False)

    # Try the information_schema approach first; if it returns no rows, fall back to SHOW TABLES + PRAGMA
    try:
        cols_df = con.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'main'
            ORDER BY table_name, ordinal_position;
        """).fetchdf()
    except Exception as e:
        print(f"WARNING: information_schema query failed: {e}", file=sys.stderr)
        cols_df = pd.DataFrame(columns=['table_name', 'column_name', 'data_type'])

    # fallback: if no rows, build cols_df from SHOW TABLES + PRAGMA_TABLE_INFO
    if cols_df.empty:
        try:
            tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
            rows = []
            for t in tables:
                try:
                    info = con.execute(f"PRAGMA_TABLE_INFO({qi(t)});").fetchall()
                    for r in info:
                        # PRAGMA_TABLE_INFO usually returns (column_name, column_type, ...)
                        colname = r[0]
                        coltype = r[1] if len(r) > 1 else None
                        rows.append({'table_name': t, 'column_name': colname, 'data_type': coltype})
                except Exception:
                    # fallback to information_schema per-table
                    try:
                        df = con.execute("""
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_schema = 'main' AND table_name = ?
                            ORDER BY ordinal_position;
                        """, [t]).fetchdf()
                        for _, rr in df.iterrows():
                            rows.append({'table_name': t, 'column_name': rr['column_name'], 'data_type': rr['data_type']})
                    except Exception:
                        pass
            cols_df = pd.DataFrame(rows, columns=['table_name', 'column_name', 'data_type'])
        except Exception as e:
            print(f"ERROR: failed to list tables/columns fallback: {e}", file=sys.stderr)
            sys.exit(1)

    migrations = {}
    report_lines = []

    for _, row in cols_df.iterrows():
        table = row['table_name']
        col = row['column_name']
        dtype = (row['data_type'] or "").upper()

        needs = False
        reason = ""
        min_val = max_val = None

        try:
            # INT-like columns (but not BIGINT) -> check MIN/MAX and fetch next non-null if needed
            if 'INT' in dtype and 'BIGINT' not in dtype:
                res = con.execute(f"SELECT MIN({qi(col)}), MAX({qi(col)}) FROM {qi(table)}").fetchone()
                min_val, max_val = res[0], res[1]

                # if MIN or MAX is None, fetch the next non-null value explicitly (asc/desc)
                if min_val is None:
                    try:
                        mv = con.execute(f"SELECT {qi(col)} FROM {qi(table)} WHERE {qi(col)} IS NOT NULL ORDER BY {qi(col)} ASC LIMIT 1").fetchone()
                        min_val = mv[0] if mv else None
                    except Exception:
                        min_val = None
                if max_val is None:
                    try:
                        mv = con.execute(f"SELECT {qi(col)} FROM {qi(table)} WHERE {qi(col)} IS NOT NULL ORDER BY {qi(col)} DESC LIMIT 1").fetchone()
                        max_val = mv[0] if mv else None
                    except Exception:
                        max_val = None

                if min_val is None and max_val is None:
                    reason = "all NULL"
                else:
                    if (max_val is not None and max_val > INT32_MAX) or (min_val is not None and min_val < INT32_MIN):
                        needs = True
                        reason = f"out-of-range INT32 ({min_val},{max_val})"

            # Floating point types: check if integer-valued and out-of-range
            elif any(k in dtype for k in ('DOUBLE','REAL','FLOAT')):
                res = con.execute(f"SELECT MIN({qi(col)}), MAX({qi(col)}), MAX(ABS({qi(col)} - CAST({qi(col)} AS BIGINT))) FROM {qi(table)}").fetchone()
                min_val, max_val, max_diff = res[0], res[1], res[2]

                # if MIN or MAX is None, fetch the next non-null value explicitly
                if min_val is None:
                    try:
                        mv = con.execute(f"SELECT {qi(col)} FROM {qi(table)} WHERE {qi(col)} IS NOT NULL ORDER BY {qi(col)} ASC LIMIT 1").fetchone()
                        min_val = mv[0] if mv else None
                    except Exception:
                        min_val = None
                if max_val is None:
                    try:
                        mv = con.execute(f"SELECT {qi(col)} FROM {qi(table)} WHERE {qi(col)} IS NOT NULL ORDER BY {qi(col)} DESC LIMIT 1").fetchone()
                        max_val = mv[0] if mv else None
                    except Exception:
                        max_val = None

                if min_val is None and max_val is None:
                    reason = "all NULL"
                else:
                    if (max_val is not None and max_val > INT32_MAX) or (min_val is not None and min_val < INT32_MIN):
                        # if values are integer-like (max_diff == 0) suggest cast to BIGINT, else warn
                        if max_diff == 0:
                            needs = True
                            reason = f"DOUBLE with integer values out-of-range ({min_val},{max_val})"
                        else:
                            reason = f"DOUBLE non-integer values out-of-range ({min_val},{max_val})"
        except Exception as ex:
            reason = f"inspect error: {ex}"

        report_lines.append({
            "table": table,
            "column": col,
            "type": dtype,
            "min": min_val,
            "max": max_val,
            "needs_migration": needs,
            "reason": reason
        })
        if needs:
            migrations.setdefault(table, []).append(col)

    # print concise report
    print("Scan report (columns that likely need migration to BIGINT marked True):")
    print("{:<30} {:<30} {:<12} {:>18} {:>18}  {}".format("table", "column", "type", "min", "max", "note"))
    for r in report_lines:
        note = r["reason"] if r["needs_migration"] else ""
        print("{:<30} {:<30} {:<12} {:>18} {:>18}  {}".format(
            r["table"], r["column"], r["type"], str(r["min"]), str(r["max"]), note
        ))

    # generate SQL if requested
    if args.generate_sql:
        print("\n-- Suggested migration SQL (do NOT run blindly; backup DB first).")
        all_tables = sorted(set(cols_df['table_name'].tolist()))
        any_suggestions = False
        for table in all_tables:
            cols_to_cast = list(migrations.get(table, []))

            # fetch all columns for the table to preserve order
            try:
                all_cols = [r[0] for r in con.execute(f"PRAGMA_TABLE_INFO({qi(table)});").fetchall()]
            except Exception:
                all_cols = cols_df[cols_df.table_name == table]['column_name'].tolist()

            # include 'id' if present (cast id to BIGINT too)
            if 'id' in all_cols and 'id' not in cols_to_cast:
                cols_to_cast.insert(0, 'id')

            if not cols_to_cast:
                continue

            any_suggestions = True
            print(f"\n-- Table: {table}")
            select_parts = []
            for c in all_cols:
                if c in cols_to_cast:
                    select_parts.append(f"CAST({qi(c)} AS BIGINT) AS {qi(c)}")
                else:
                    select_parts.append(qi(c))
            select_sql = ",\n    ".join(select_parts)
            print(f"CREATE TABLE {qi(table)}_new AS\nSELECT\n    {select_sql}\nFROM {qi(table)};")
            print(f"/* verify row counts: SELECT COUNT(*) FROM {qi(table)}; SELECT COUNT(*) FROM {qi(table)}_new; */")
            print(f"DROP TABLE {qi(table)};")
            print(f"ALTER TABLE {qi(table)}_new RENAME TO {qi(table)};")

        if not any_suggestions:
            print("\nNo migrations suggested (no tables with 'id' or problematic integer columns found).")

    con.close()

if __name__ == "__main__":
    main()