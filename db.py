import duckdb
from pathlib import Path

from bronze import RawEvent


class Persistence:

    def __init__(self, duckdb_file: Path):
        self.CONN = duckdb.connect(str(duckdb_file.resolve()))

    def _load_table(self, table: str, jsonlfile: Path):

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
                SELECT * FROM read_json_auto(?)
            """

        return self.CONN.execute(query, (str(jsonlfile),))

    def _vacuum(self):
        self.CONN.execute("VACUUM")

    def store_raw_events(self, jsonlfile: Path):
        return self._load_table('raw_events', jsonlfile)

    def store_schema_events(self, jsonlfile: Path):
        return self._load_table('schema_events', jsonlfile)
