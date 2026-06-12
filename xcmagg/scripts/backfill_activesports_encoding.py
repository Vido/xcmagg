"""
One-time cleanup: fix mojibake in ActiveSports titles.

Cause: UTF-8 file was read as iso-8859-1, producing garbled text.
  Before: "2Âª ETAPA ALIGA MTB 2026"
  After:  "2ª ETAPA ALIGA MTB 2026"

Fix: encode back to latin-1, re-decode as UTF-8.

Usage:
  uv run python3 scripts/backfill_activesports_encoding.py          # run
  uv run python3 scripts/backfill_activesports_encoding.py rollback # restore backup
"""
import sys
import shutil
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

# ── Helpers ──────────────────────────────────────────────────────────────────
def fix_mojibake(s: str) -> str:
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s  # already clean

import duckdb

conn = duckdb.connect(str(DB_PATH))

try:
    for table in ('raw_events', 'schema_events'):
        rows = conn.execute(f"""
            SELECT rowid, title FROM {table}
            WHERE source = 'https://www.activesports.com.br/'
        """).fetchall()

        updates = []
        for rowid, title in rows:
            fixed = fix_mojibake(title)
            if fixed != title:
                updates.append((fixed, rowid))

        if not updates:
            print(f"{table}: nothing to fix")
            continue

        conn.executemany(f"UPDATE {table} SET title = ? WHERE rowid = ?", updates)
        print(f"{table}: fixed {len(updates)} rows")
        for clean, rowid in updates[:5]:
            original = next(t for r, t in rows if r == rowid)
            print(f"  [{rowid}] {original!r} → {clean!r}")
        if len(updates) > 5:
            print(f"  ... and {len(updates) - 5} more")

    # Verify: no rows should still be fixable by fix_mojibake
    check_rows = conn.execute("""
        SELECT rowid, title FROM schema_events
        WHERE source = 'https://www.activesports.com.br/'
    """).fetchall()
    still_fixable = [(rowid, title) for rowid, title in check_rows if fix_mojibake(title) != title]
    if still_fixable:
        for rowid, title in still_fixable:
            print(f"  Still fixable [{rowid}]: {title!r}")
        raise RuntimeError(f"Verification failed: {len(still_fixable)} rows still fixable in schema_events")

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
