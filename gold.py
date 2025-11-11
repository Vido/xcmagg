from pathlib import Path
from dataclasses import dataclass, asdict

from silver import DateRange, Location
from db import Persistence


class GoldLayer:

    @classmethod
    def publish(klass):
        p = Persistence()
        output_file = str((Path(__file__).parent / 'data' / 'gold' / 'data.jsonl').resolve())
        results = p.CONN.execute(
            f"""
                COPY (
                    SELECT
                        title,
                        CASE
                        WHEN POSITION('?' IN url) > 0 THEN url || '&utm_source=xcmagg'
                        ELSE url || '?utm_source=xcmagg'
                        END AS url,
                        -- STRPTIME(date_range->>'start_date', '%d-%m-%Y') AS start_date,
                        date_range->>'start_date' AS date,
                        location->>'city' AS city,
                        location->>'uf' AS uf
                FROM schema_events
                WHERE TRY_CAST(date_range->>'start_date' AS DATE) > CURRENT_DATE
                ) TO '{ output_file }' (FORMAT JSON, ARRAY false);
            """
        )
        return results
