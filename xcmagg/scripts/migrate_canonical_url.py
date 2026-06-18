"""One-time schema migration: add + populate `canonical_url` on raw_events and
schema_events, then collapse schema_events duplicates that share one.

The pipeline now derives `canonical_url` at bronze time and dedups silver on it
(Extractor.canonical_url / bronze.canonical_url). Tables created before this
change lack the column, so db._store_data's `MERGE ... INSERT */UPDATE SET *`
would hit a column-count mismatch. Run this once before the first crawl on the
new code.

Usage:
  uv run python3 scripts/migrate_canonical_url.py          # run
  uv run python3 scripts/migrate_canonical_url.py rollback # restore backup
"""
import sys
import shutil
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).parent.parent))

from bronze import canonical_url as generic_canonical_url
from aggregators import canonical_ticketsports_url

DB_PATH = Path('data/events.duckdb')
BACKUP_PATH = Path('data/events.duckdb.bak')


def canonical_url(url: str) -> str:
    """Re-derive a row's canonical_url. Routes by host since the migration has
    no Extractor instance — mirrors the per-source Extractor.canonical_url."""
    if url and 'ticketsports.com.br' in urlsplit(url).netloc.lower():
        return canonical_ticketsports_url(url)
    return generic_canonical_url(url)


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

# ── Migrate ──────────────────────────────────────────────────────────────────
import duckdb

conn = duckdb.connect(str(DB_PATH))


def column_exists(table: str, col: str) -> bool:
    return conn.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name = ? AND column_name = ?
    """, [table, col]).fetchone()[0] > 0


def populate(table: str):
    if not column_exists(table, 'canonical_url'):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN canonical_url VARCHAR")
        print(f"{table}: added canonical_url column")

    urls = [r[0] for r in conn.execute(
        f"SELECT DISTINCT url FROM {table} WHERE url IS NOT NULL"
    ).fetchall()]
    mapping = [(canonical_url(u), u) for u in urls]
    conn.executemany(
        f"UPDATE {table} SET canonical_url = ? WHERE url = ?", mapping
    )
    print(f"{table}: populated canonical_url for {len(mapping)} distinct urls")


try:
    populate('raw_events')
    populate('schema_events')

    # Collapse schema_events duplicates: keep latest processed_at per
    # canonical_url (tie-break on url for a deterministic single survivor).
    before = conn.execute("SELECT COUNT(*) FROM schema_events").fetchone()[0]
    conn.execute("""
        DELETE FROM schema_events
        WHERE rowid IN (
            SELECT rowid FROM (
                SELECT rowid, ROW_NUMBER() OVER (
                    PARTITION BY canonical_url ORDER BY processed_at DESC, url DESC
                ) AS rn
                FROM schema_events
            ) WHERE rn > 1
        )
    """)
    after = conn.execute("SELECT COUNT(*) FROM schema_events").fetchone()[0]
    print(f"schema_events: removed {before - after} duplicate rows")

    remaining = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT canonical_url FROM schema_events
            GROUP BY canonical_url HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    if remaining > 0:
        raise RuntimeError(f"Verification failed: {remaining} duplicate canonical_url groups remain")

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
