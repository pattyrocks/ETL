from config import DRY_RUN
from utils import log_and_print


def check_and_remove_duplicates(con, table, partition_cols, select_cols):
    """Generic deduplication for any table."""
    log_and_print(f"Checking for duplicates in {table}...")

    partition_str = ', '.join(partition_cols)

    try:
        dup_count = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {partition_str}
                FROM {table}
                GROUP BY {partition_str}
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
    except Exception:
        log_and_print(f"{table} table doesn't exist or is empty")
        return

    if dup_count == 0:
        log_and_print(f"No duplicates found in {table}.")
        return

    log_and_print(f"Found {dup_count} duplicate groups in {table}. Removing...")

    if DRY_RUN:
        log_and_print(f"[DRY RUN] Would remove duplicates from {table}")
        return

    select_cols_str = ', '.join(select_cols)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table}_dedup AS
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_str}
                    ORDER BY inserted_at DESC
                ) AS rn
            FROM {table}
        )
        WHERE rn = 1
    """)

    before_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    con.execute(f"DELETE FROM {table}")
    con.execute(f"""
        INSERT INTO {table}
        SELECT {select_cols_str}
        FROM {table}_dedup
    """)

    after_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    deleted = before_count - after_count

    con.execute(f"DROP TABLE IF EXISTS {table}_dedup")
    log_and_print(f"{table} deduplication complete. Removed {deleted} rows.")
