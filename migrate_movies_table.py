"""
Generalized migration helper for DuckDB tables to cast problematic integer-like columns to BIGINT.

What it does:
- Creates a filesystem backup copy of the DuckDB database file.
- Scans all tables (or a provided subset) for columns that need migration:
  - INT columns that are not BIGINT and have values outside INT32 range.
  - DOUBLE/REAL/FLOAT columns that contain integer-like values outside INT32 range.
- For each affected table, constructs a CREATE TABLE <table>_new AS SELECT ... where
  affected columns (and 'id' if present) are CAST(... AS BIGINT).
- Verifies row counts, swaps tables atomically, and verifies column types.
- On any failure, restores the DB from the backup copy (unless --no-restore).
- Optionally prints the generated SQL rather than executing it (--dry-run).

Usage:
  python migrate_movies_table.py --db TMDB
  python migrate_movies_table.py --db TMDB --dry-run --generate-sql
"""
import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import duckdb

INT32_MIN = -2_147_483_648
INT32_MAX = 2_147_483_647

def timestamp():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def backup_db(db_path: Path) -> Path:
    if not db_path.exists():
        raise FileNotFoundError(f"DB file not found: {db_path}")
    bak = db_path.with_name(f"{db_path.name}.bak.{timestamp()}")
    shutil.copy2(db_path, bak)
    return bak

def restore_backup(backup_path: Path, db_path: Path):
    if backup_path.exists():
        shutil.copy2(backup_path, db_path)

def qi(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'

def describe_table(con, tbl: str):
    """
    Return list of (column_name, data_type) for table `tbl`.
    Tries information_schema first, falls back to PRAGMA_TABLE_INFO.
    """
    try:
        df = con.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'main' AND table_name = ?
            ORDER BY ordinal_position;
        """, [tbl]).fetchdf()
        if not df.empty:
            return [(r['column_name'], r['data_type']) for _, r in df.iterrows()]
    except Exception:
        pass

    try:
        # PRAGMA_TABLE_INFO may return (name, type, ...)
        rows = con.execute(f"PRAGMA_TABLE_INFO({qi(tbl)});").fetchall()
        res = []
        for r in rows:
            name = r[0] if len(r) > 0 else None
            typ = r[1] if len(r) > 1 else None
            res.append((name, typ))
        return res
    except Exception:
        return []

def table_columns(con, tbl: str):
    try:
        rows = con.execute(f"PRAGMA_TABLE_INFO({qi(tbl)});").fetchall()
        # PRAGMA_TABLE_INFO returns tuples where first element is name in many DuckDB versions
        return [r[0] for r in rows]
    except Exception:
        # fallback to information_schema
        df = con.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'main' AND table_name = ?
            ORDER BY ordinal_position;
        """, [tbl]).fetchdf()
        return [r['column_name'] for _, r in df.iterrows()]

def column_min_max(con, table: str, col: str):
    # return (min, max, max_diff_for_float)
    try:
        # try numeric MIN/MAX; for float also compute max diff to integer
        res = con.execute(f"SELECT MIN({qi(col)}), MAX({qi(col)}) FROM {qi(table)}").fetchone()
        min_val, max_val = res[0], res[1]
        # if NULLs returned, fetch first non-null values
        if min_val is None:
            mv = con.execute(f"SELECT {qi(col)} FROM {qi(table)} WHERE {qi(col)} IS NOT NULL ORDER BY {qi(col)} ASC LIMIT 1").fetchone()
            min_val = mv[0] if mv else None
        if max_val is None:
            mv = con.execute(f"SELECT {qi(col)} FROM {qi(table)} WHERE {qi(col)} IS NOT NULL ORDER BY {qi(col)} DESC LIMIT 1").fetchone()
            max_val = mv[0] if mv else None
        max_diff = None
        # compute max diff for floats if needed
        return min_val, max_val, max_diff
    except Exception:
        return None, None, None

def inspect_table_for_migration(con, table: str):
    cols = describe_table(con, table)
    to_cast = []
    report = []
    for name, typ in cols:
        dtype = (typ or "").upper()
        min_val = max_val = max_diff = None
        reason = ""
        needs = False
        try:
            if 'INT' in dtype and 'BIGINT' not in dtype:
                min_val, max_val, _ = column_min_max(con, table, name)
                if min_val is None and max_val is None:
                    reason = "all NULL"
                else:
                    if (max_val is not None and max_val > INT32_MAX) or (min_val is not None and min_val < INT32_MIN):
                        needs = True
                        reason = f"out-of-range INT32 ({min_val},{max_val})"
            elif any(k in dtype for k in ('DOUBLE','REAL','FLOAT')):
                # check min/max and integer-ness
                try:
                    res = con.execute(f"SELECT MIN({qi(name)}), MAX({qi(name)}), MAX(ABS({qi(name)} - CAST({qi(name)} AS BIGINT))) FROM {qi(table)}").fetchone()
                    min_val, max_val, max_diff = res[0], res[1], res[2]
                    if min_val is None and max_val is None:
                        reason = "all NULL"
                    else:
                        if (max_val is not None and max_val > INT32_MAX) or (min_val is not None and min_val < INT32_MIN):
                            if max_diff == 0:
                                needs = True
                                reason = f"DOUBLE integer-like out-of-range ({min_val},{max_val})"
                            else:
                                reason = f"DOUBLE non-integer values out-of-range ({min_val},{max_val})"
                except Exception:
                    min_val, max_val, max_diff = column_min_max(con, table, name)
        except Exception as ex:
            reason = f"inspect error: {ex}"
        report.append((name, dtype, min_val, max_val, needs, reason))
        if needs:
            to_cast.append(name)
    return to_cast, report

def generate_migration_sql(con, table: str, cols_to_cast):
    all_cols = table_columns(con, table)
    # ensure 'id' is included if present
    if 'id' in all_cols and 'id' not in cols_to_cast:
        cols_to_cast.insert(0, 'id')
    select_parts = []
    for c in all_cols:
        if c in cols_to_cast:
            select_parts.append(f"CAST({qi(c)} AS BIGINT) AS {qi(c)}")
        else:
            select_parts.append(qi(c))
    select_sql = ",\n    ".join(select_parts)

    new_table = f"{table}_new"
    sql = f"""CREATE TABLE {qi(new_table)} AS
SELECT
    {select_sql}
FROM {qi(table)};"""
    swap_sql = f"""-- verify row counts:
-- SELECT COUNT(*) FROM {qi(table)};
-- SELECT COUNT(*) FROM {qi(new_table)};
DROP TABLE {qi(table)};
ALTER TABLE {qi(new_table)} RENAME TO {qi(table)};"""
    return sql, swap_sql

def run_table_migration(con, table: str, cols_to_cast):
    create_sql, swap_sql = generate_migration_sql(con, table, list(cols_to_cast))
    con.execute("BEGIN TRANSACTION;")
    con.execute(create_sql)
    old_count = con.execute(f"SELECT COUNT(*) FROM {qi(table)};").fetchone()[0]
    new_count = con.execute(f"SELECT COUNT(*) FROM {qi(table + '_new')};").fetchone()[0]
    if old_count != new_count:
        con.execute("ROLLBACK;")
        raise RuntimeError(f"Row count mismatch for table {table}: old={old_count} new={new_count}")
    con.execute(f"DROP TABLE {qi(table)};")
    con.execute(f"ALTER TABLE {qi(table + '_new')} RENAME TO {qi(table)};")
    con.execute("COMMIT;")
    return old_count, new_count

def column_safe_to_bigint(con, table: str, col: str, allow_fraction_nonconv: float = 0.0) -> bool:
    """
    Return True if the non-null values in table.col are safely convertible to BIGINT.
    Uses TRY_CAST in SQL when available; falls back to sampling values and testing in Python.
    allow_fraction_nonconv: fraction of non-null values allowed to be non-convertible (default 0.0 -> none).
    """
    try:
        # TRY_CAST returns NULL for non-convertible values in DuckDB
        q = f"""
        SELECT
          SUM(CASE WHEN {qi(col)} IS NOT NULL AND TRY_CAST({qi(col)} AS BIGINT) IS NULL THEN 1 ELSE 0 END) AS nonconv,
          SUM(CASE WHEN {qi(col)} IS NOT NULL THEN 1 ELSE 0 END) AS nonnull
        FROM {qi(table)}
        """
        nonconv, nonnull = con.execute(q).fetchone()
        nonconv = int(nonconv or 0)
        nonnull = int(nonnull or 0)
        if nonnull == 0:
            return True
        return (nonconv / nonnull) <= allow_fraction_nonconv
    except Exception:
        # Fallback: sample distinct non-null values and test in Python
        try:
            rows = con.execute(f"SELECT DISTINCT {qi(col)} FROM {qi(table)} WHERE {qi(col)} IS NOT NULL LIMIT 200").fetchall()
            if not rows:
                return True
            nonconv = 0
            total = 0
            for (v,) in rows:
                total += 1
                try:
                    # accept ints and float-like integer values
                    if isinstance(v, (int,)) :
                        continue
                    if isinstance(v, float) and v.is_integer():
                        continue
                    # if string that looks like integer
                    if isinstance(v, str):
                        vs = v.strip()
                        # allow leading +/-
                        if vs.startswith(('+','-')):
                            vs2 = vs[1:]
                        else:
                            vs2 = vs
                        if vs2.isdigit():
                            continue
                    nonconv += 1
                except Exception:
                    nonconv += 1
            if total == 0:
                return True
            return (nonconv / total) <= allow_fraction_nonconv
        except Exception:
            return False

def main():
    p = argparse.ArgumentParser(description="Migrate integer-like columns across tables to BIGINT with backup/restore.")
    p.add_argument("--db", default="TMDB", help="Path to DuckDB database file (default: TMDB)")
    p.add_argument("--tables", nargs="*", help="Specific tables to inspect/migrate (default: all tables)")
    p.add_argument("--dry-run", action="store_true", help="Do not execute migrations; print SQL and report only")
    p.add_argument("--generate-sql", action="store_true", help="Print generated migration SQL for affected tables")
    p.add_argument("--no-restore", action="store_true", help="Do not auto-restore backup on failure")
    args = p.parse_args()

    db_path = Path(args.db)
    print(f"DB path: {db_path.resolve()}")

    try:
        backup = backup_db(db_path)
        print(f"Backup created at: {backup}")
    except Exception as e:
        print(f"ERROR: could not create backup: {e}")
        sys.exit(1)

    con = None
    try:
        con = duckdb.connect(database=str(db_path), read_only=False)
        # list tables robustly
        try:
            tbls_df = con.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'main'
                ORDER BY table_name;
            """).fetchdf()
            all_tables = [r['table_name'] for _, r in tbls_df.iterrows()] if not tbls_df.empty else []
        except Exception:
            # fallback to SHOW TABLES
            all_tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]

        if args.tables:
            tables = [t for t in args.tables if t in all_tables]
        else:
            tables = all_tables

        if not tables:
            print("No tables found to inspect.")
            return

        overall_migrations = {}
        for table in tables:
            cols_to_cast, report = inspect_table_for_migration(con, table)

            # ensure we include the canonical 'id' and always include budget/revenue if present
            # (user confirmed these should be migrated). Also include any "id-like" columns
            # (columns equal to 'id', ending with '_id', or containing 'credit' + 'id', e.g. credit_id, cast_id).
            all_cols = table_columns(con, table)
            lower_map = {c.lower(): c for c in all_cols}
            forced = []

            # always force budget/revenue if present
            for k in ('budget', 'revenue'):
                if k in lower_map and lower_map[k] not in cols_to_cast:
                    forced.append(lower_map[k])

            # detect id-like columns and force them, but only if values are convertible
            id_like = []
            for c in all_cols:
                lc = c.lower()
                if lc == 'id' or lc.endswith('_id') or ('credit' in lc and 'id' in lc) or ('cast' in lc and lc.endswith('_id')):
                    id_like.append(c)

            # only force id-like columns that are safe to cast to BIGINT
            safe_id_like = []
            for c in id_like:
                # skip if already detected for casting
                if c in cols_to_cast:
                    continue
                try:
                    if column_safe_to_bigint(con, table, c):
                        safe_id_like.append(c)
                except Exception:
                    # be conservative: skip unsafe columns
                    continue

            # ensure uniqueness and preserve order: existing detected cols first, then safe id-like, then budget/revenue forced
            for c in safe_id_like:
                if c not in cols_to_cast and c not in forced:
                    forced.append(c)

            # merge detected and forced, preserving order and uniqueness
            if forced:
                merged = []
                for c in cols_to_cast + forced:
                    if c not in merged:
                        merged.append(c)
                cols_to_cast = merged

            if cols_to_cast:
                overall_migrations[table] = cols_to_cast

            # brief per-table report
            if report:
                print(f"\nTable: {table}")
                for name, dtype, mn, mx, needs, reason in report:
                    mark = "MIGRATE" if needs else ""
                    print(f"  {name:30} {dtype:12} min={str(mn):>12} max={str(mx):>12} {mark:10} {reason}")
                if forced:
                    print(f"  Forced casts applied for this run: {forced}")

        if not overall_migrations:
            print("\nNo migrations suggested (no columns found needing BIGINT).")
            con.close()
            return

        print("\nSuggested migrations:")
        for t, cols in overall_migrations.items():
            print(f"  {t}: {cols}")

        if args.generate_sql or args.dry_run:
            print("\n-- Generated SQL preview:")
            for t, cols in overall_migrations.items():
                sql, swap = generate_migration_sql(con, t, list(cols))
                print(f"\n-- Table: {t}\n{sql}\n{swap}")
            if args.dry_run:
                print("\nDry-run requested; exiting without applying migrations.")
                con.close()
                return

        # apply migrations one table at a time, restore on first failure
        for t, cols in overall_migrations.items():
            print(f"\nApplying migration for table: {t} (casting: {cols})")
            old_count, new_count = run_table_migration(con, t, cols)
            print(f"  Migration applied: rows old={old_count} new={new_count}")

        # final verification
        print("\nAll migrations applied. Verifying types for migrated columns...")
        for t, cols in overall_migrations.items():
            desc = describe_table(con, t)
            # build mapping name.lower() -> type (uppercase) or 'UNKNOWN'
            types = { (n.lower() if n else ''): ( (typ.upper() if typ else 'UNKNOWN') ) for n, typ in desc }
            for c in cols:
                key = c.lower()
                typ = types.get(key)
                if typ and 'BIGINT' in typ:
                    print(f"  OK: {t}.{c} -> {typ}")
                else:
                    print(f"  WARNING: {t}.{c} type is {typ}, expected BIGINT")

        con.close()
        print("\nMigration run finished successfully.")
    except Exception as err:
        print(f"Migration failed: {err}")
        if con:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            con.close()
        if not args.no_restore:
            try:
                restore_backup(backup, db_path)
                print(f"Restored DB from backup: {backup} -> {db_path}")
            except Exception as e:
                print(f"Failed to restore backup automatically: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()