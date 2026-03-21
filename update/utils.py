import time
import pickle
from update.config import (
    logger, DRY_RUN, SAMPLE_SIZE, RATE_LIMIT_RETRY_DELAY
)


def log_and_print(message, level='info'):
    getattr(logger, level)(message)


def handle_rate_limit(attempt):
    wait_time = RATE_LIMIT_RETRY_DELAY * (2 ** attempt)
    log_and_print(f"Rate limited. Waiting {wait_time}s before retry...", level='warning')
    time.sleep(wait_time)


def save_checkpoint(processed_ids, filename):
    if DRY_RUN:
        log_and_print(f"[DRY RUN] Would save checkpoint to {filename}")
        return
    try:
        with open(filename, 'wb') as f:
            pickle.dump(processed_ids, f)
    except IOError as e:
        log_and_print(f"Failed to save checkpoint {filename}: {e}", level='error')


def load_checkpoint(filename):
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return set()
    except Exception as e:
        log_and_print(f"Failed to load checkpoint {filename}: {e}", level='warning')
        return set()


def log_null_columns(df, log_file):
    if df.empty:
        return
    null_counts = df.isnull().sum()
    try:
        with open(log_file, 'w') as f:
            for col, count in null_counts.items():
                if count > 0:
                    f.write(f"{col}: {count} nulls\n")
        log_and_print(f"Null column log written to {log_file}")
    except IOError as e:
        log_and_print(f"Failed to write null column log: {e}", level='error')


def log_skipped_ids(skipped_ids, filename):
    if not skipped_ids:
        return
    try:
        with open(filename, 'w') as f:
            for sid in skipped_ids:
                f.write(f"{sid}\n")
        log_and_print(f"Wrote {len(skipped_ids)} skipped IDs to {filename}", level='warning')
    except IOError as e:
        log_and_print(f"Failed to write skipped IDs log: {e}", level='error')


def safe_str(value):
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return str(value)
    return str(value) if value else None


def apply_sample(ids_list):
    """Apply sample limit if set."""
    if SAMPLE_SIZE > 0 and len(ids_list) > SAMPLE_SIZE:
        log_and_print(f"[SAMPLE] Limiting from {len(ids_list)} to {SAMPLE_SIZE} items")
        return ids_list[:SAMPLE_SIZE]
    return ids_list
