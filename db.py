import duckdb
from pathlib import Path

from bronze import RawEvent


class Persistence:

    BASE = Path(__file__).parent / 'data'

    def __init__(self):
        self.CONN = duckdb.connect(str(self.BASE / 'events.duckdb'))

    def _store_data(self, table: str, jsonlfile: Path):

        if not table.isidentifier():
            raise ValueError(f"Invalid table name: {table}")

        exists = self.CONN.execute(
            """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = ?
            """,
            [table]
        ).fetchone()[0] > 0

        query = f"""
            MERGE INTO {table} AS t
            USING read_json_auto(?) AS s
            ON t.url = s.url
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *;
        """

        if not exists:
            query = f"""
                CREATE TABLE {table} AS
                SELECT * FROM read_json_auto(?);
            """

        return self.CONN.execute(query, (str(jsonlfile),))

    def _vacuum(self):
        self.CONN.execute("VACUUM")

    def store_raw_events(self, jsonlfile: Path):
        return self._store_data('raw_events', jsonlfile)

    def load_all_events(self):
        rows = self.CONN.execute("SELECT * FROM raw_events").fetchall()
        cols = [c[0] for c in self.CONN.description]
        data = [dict(zip(cols, row)) for row in rows]
        return data

    # Dedup
    def load_new_events(self):
        rows = self.CONN.execute("""
        SELECT r.*
            FROM raw_events r
            LEFT JOIN schema_events s
            ON r.url = s.url
            WHERE s.url IS NULL;
        """).fetchall()
        cols = [c[0] for c in self.CONN.description]
        data = [dict(zip(cols, row)) for row in rows]
        return data

    def store_schema_events(self, jsonlfile: Path):
        return self._store_data('schema_events', jsonlfile)


