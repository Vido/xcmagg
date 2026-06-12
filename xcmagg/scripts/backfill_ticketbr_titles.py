"""
One-time cleanup: strip "Evento: DD/MM/YYYY " prefix from TicketBR titles
in raw_events and schema_events.

Before: "Evento: 08/07/2026 PEPE NIGHT RUN 5K Pirassununga SP"
After:  "PEPE NIGHT RUN 5K Pirassununga SP"

Also handles entries without a date:
Before: "Evento: Atenção NOVA DATA 2026 Race MTB"
After:  "Atenção NOVA DATA 2026 Race MTB"

Usage:
  uv run python3 scripts/backfill_ticketbr_titles.py          # run
  uv run python3 scripts/backfill_ticketbr_titles.py rollback # restore backup
"""
import sys
import re
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

DB_PATH = Path('data/events.duckdb')
BACKUP_PATH = Path('data/events.duckdb.bak')
PATTERN = re.compile(r'^Evento:\s*(?:\d{2}/\d{2}/\d{4}\s*-?\s*)?')

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

try:
    for table in ('raw_events', 'schema_events'):
        rows = conn.execute(f"""
            SELECT rowid, title FROM {table}
            WHERE title LIKE 'Evento:%'
        """).fetchall()

        if not rows:
            print(f"{table}: no rows to update")
            continue

        updates = [(PATTERN.sub('', title).strip(), rowid) for rowid, title in rows]
        conn.executemany(f"UPDATE {table} SET title = ? WHERE rowid = ?", updates)
        print(f"{table}: updated {len(updates)} rows")
        for _, rowid in updates[:5]:
            original = next(t for r, t in rows if r == rowid)
            clean = PATTERN.sub('', original).strip()
            print(f"  [{rowid}] {original!r} → {clean!r}")
        if len(updates) > 5:
            print(f"  ... and {len(updates) - 5} more")

    # Verify: no titles starting with "Evento:" should remain
    for table in ('raw_events', 'schema_events'):
        remaining = conn.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE title LIKE 'Evento:%'
        """).fetchone()[0]
        if remaining > 0:
            raise RuntimeError(f"Verification failed: {remaining} rows still have 'Evento:' prefix in {table}")

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
