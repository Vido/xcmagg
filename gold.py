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
                        e.title,
                        CASE
                        WHEN POSITION('?' IN e.url) > 0 THEN e.url || '&utm_source=racefeed'
                        ELSE e.url || '?utm_source=racefeed'
                        END AS url,
                        STRFTIME(
                            STRPTIME(e.date_range->>'start_date', '%Y-%m-%d'),
                            '%d-%m-%Y') AS start_date,
                        e.location->>'city' AS city,
                        e.location->>'uf' AS uf,
                        g.ddd::VARCHAR AS ddd,
                        g.latitude,
                        g.longitude,
                        e.sport
                    FROM schema_events e
                    LEFT JOIN geo g
                        ON LOWER(TRIM(e.location->>'city')) = LOWER(TRIM(g.nome))
                        AND UPPER(TRIM(e.location->>'uf')) = UPPER(TRIM(g.uf))
                    WHERE TRY_CAST(e.date_range->>'start_date' AS DATE) > CURRENT_DATE
                ) TO '{ output_file }' (FORMAT JSON, ARRAY false);
            """
        )
        return results
