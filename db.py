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
            USING (
                SELECT * FROM read_json_auto(?)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY url ORDER BY crawled_at DESC) = 1
            ) AS s
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

        print('jsonlfile:', jsonlfile)
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
        SELECT * FROM (
            SELECT r.*, ROW_NUMBER() OVER (PARTITION BY r.url ORDER BY r.crawled_at DESC) AS rn
            FROM raw_events r
            LEFT JOIN schema_events s ON r.url = s.url
            WHERE s.url IS NULL
        ) WHERE rn = 1;
        """).fetchall()
        cols = [c[0] for c in self.CONN.description]
        data = [dict(zip(cols, row)) for row in rows]
        for d in data:
            d.pop('rn', None)
        return data

    def load_low_quality_events(self):
        rows = self.CONN.execute("""
        SELECT r.* FROM (
            SELECT r.*, ROW_NUMBER() OVER (PARTITION BY r.url ORDER BY r.crawled_at DESC) AS rn
            FROM raw_events r
            JOIN schema_events s ON r.url = s.url
            WHERE (s.sport = '' OR s.location.confidence = 'low')
              AND TRY_CAST(s.date_range->>'start_date' AS DATE) > CURRENT_DATE
        ) r WHERE rn = 1;
        """).fetchall()
        cols = [c[0] for c in self.CONN.description]
        data = [dict(zip(cols, row)) for row in rows]
        for d in data:
            d.pop('rn', None)
        return data

    def store_schema_events(self, jsonlfile: Path):
        return self._store_data('schema_events', jsonlfile)

    def load_geo(self):
        geo_file = str(self.BASE / 'geo' / 'municipios_ibge.json')
        self.CONN.execute(f"""
            CREATE OR REPLACE TABLE geo AS
            SELECT * FROM read_json_auto('{geo_file}')
        """)


