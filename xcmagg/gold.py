from pathlib import Path
from dataclasses import dataclass, asdict

from silver import DateRange, Location
from db import Persistence


class GoldLayer:

    @classmethod
    def publish(klass):
        p = Persistence()
        junk_file = str((Path(__file__).parent.parent / 'data' / 'junk_urls.txt').resolve())
        output_file = str((Path(__file__).parent.parent / 'data' / 'gold' / 'data.jsonl').resolve())
        results = p.CONN.execute(
            f"""
                COPY (
                    SELECT
                        e.title,
                        CASE
                        WHEN POSITION('?' IN e.canonical_url) > 0 THEN e.canonical_url || '&utm_source=racefeed'
                        ELSE e.canonical_url || '?utm_source=racefeed'
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
                        ON strip_accents(LOWER(TRIM(e.location->>'city'))) = strip_accents(LOWER(TRIM(g.nome)))
                        AND UPPER(TRIM(e.location->>'uf')) = UPPER(TRIM(g.uf))
                    WHERE TRY_CAST(e.date_range->>'start_date' AS DATE) > CURRENT_DATE
                    AND e.canonical_url NOT IN (
                        SELECT column0
                        FROM read_csv('{ junk_file }', header=false, comment='#')
                        WHERE trim(column0) != ''
                    )
                ) TO '{ output_file }' (FORMAT JSON, ARRAY false);
            """
        )
        return results
