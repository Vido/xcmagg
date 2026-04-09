"""
One-time cleanup: remove old slug-format TicketSports URLs from raw_events and schema_events,
keeping only the new URL-encoded format (with + and uppercase) from APIv2.

Old (slug):    .../e/18a-copa-interior-de-triathlon-3a-etapa-sorocaba-74202
New (encoded): .../e/18%C2%AA+COPA+INTERIOR+DE+TRIATHLON+3%C2%AA+ETAPA+-++SOROCABA-74202

Usage:
  uv run python3 scripts/backfill_dedup_ticketsports.py          # run
  uv run python3 scripts/backfill_dedup_ticketsports.py rollback # restore backup
"""
import sys
import shutil
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

DB_PATH = Path('data/events.duckdb')
BACKUP_PATH = Path('data/events.duckdb.bak')

# ── Rollback mode ────────────────────────────────────────────────────────────
if len(sys.argv) > 1 and sys.argv[1] == 'rollback':
    if not BACKUP_PATH.exists():
        print("No backup found at", BACKUP_PATH)
        sys.exit(1)
    shutil.copy2(BACKUP_PATH, DB_PATH)
    print(f"Rolled back: {BACKUP_PATH} → {DB_PATH}")
    sys.exit(0)

# ── Backup ───────────────────────────────────────────────────────────────────
shutil.copy2(DB_PATH, BACKUP_PATH)
print(f"Backup created: {BACKUP_PATH}")

# ── Cleanup ──────────────────────────────────────────────────────────────────
import duckdb

conn = duckdb.connect(str(DB_PATH))

SLUG_PATTERN = r'https://www\.ticketsports\.com\.br/e/[a-z0-9-]+-\d+$'

# Only delete slug URLs that have a counterpart encoded URL for the same event
# (don't delete slug URLs that are the only record for an event)
slug_urls = conn.execute(f"""
    SELECT s.url
    FROM schema_events s
    WHERE regexp_matches(s.url, '{SLUG_PATTERN}')
      AND EXISTS (
          SELECT 1 FROM schema_events s2
          WHERE s2.url != s.url
            AND LOWER(TRIM(s2.title))             = LOWER(TRIM(s.title))
            AND s2.date_range.start_date::VARCHAR  = s.date_range.start_date::VARCHAR
            AND LOWER(TRIM(s2.location->>'city')) = LOWER(TRIM(s.location->>'city'))
            AND UPPER(TRIM(s2.location->>'uf'))   = UPPER(TRIM(s.location->>'uf'))
      )
""").fetchall()

urls = [r[0] for r in slug_urls]
print(f"Found {len(urls)} old slug URLs to delete")
for u in urls:
    print(f"  {u}")

if not urls:
    print("Nothing to do.")
    sys.exit(0)

try:
    conn.execute("DELETE FROM schema_events WHERE url = ANY(?)", [urls])
    print(f"schema_events: deleted {len(urls)} rows")

    raw_count = conn.execute("SELECT COUNT(*) FROM raw_events WHERE url = ANY(?)", [urls]).fetchone()[0]
    if raw_count:
        conn.execute("DELETE FROM raw_events WHERE url = ANY(?)", [urls])
        print(f"raw_events: deleted {raw_count} rows")
    else:
        print("raw_events: no matching rows")

    # Verify: no duplicates should remain for any of the deleted events
    remaining = conn.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT LOWER(TRIM(title)), date_range.start_date::VARCHAR,
                   LOWER(TRIM(location->>'city')), UPPER(TRIM(location->>'uf'))
            FROM schema_events
            WHERE regexp_matches(url, '{SLUG_PATTERN}')
            GROUP BY LOWER(TRIM(title)), date_range.start_date::VARCHAR,
                     LOWER(TRIM(location->>'city')), UPPER(TRIM(location->>'uf'))
            HAVING COUNT(*) > 1

        )
    """).fetchone()[0]

    if remaining > 0:
        raise RuntimeError(f"Verification failed: {remaining} duplicate groups still exist")

    conn.execute("VACUUM")
    print("Verification passed. Done.")
    print(f"Backup kept at {BACKUP_PATH} — delete it manually when satisfied.")

except Exception as e:
    conn.close()
    print(f"\nERROR: {e}")
    print("Restoring backup...")
    shutil.copy2(BACKUP_PATH, DB_PATH)
    print("Rollback complete.")
    sys.exit(1)
