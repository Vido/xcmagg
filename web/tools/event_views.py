"""SSR landing pages for event location/discipline combinations.

Data source: settings.DATA_DIR / 'events.duckdb'
Tables: schema_events (structured events), geo (IBGE municipalities with lat/lon).

Wired in events_urls.py behind settings.DEBUG until verified in prod.
TODO: replace DEBUG gate with a proper feature flag before going live.
"""
import locale
from collections import Counter
from datetime import date
from pathlib import Path

import duckdb
from urllib.parse import urlparse
from django.conf import settings
from django.http import Http404
from django.shortcuts import render
from django.utils.text import slugify

try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except locale.Error:
    pass  # fall back to system locale; month abbrevs may be English

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

UF_NAMES = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
    'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo',
    'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
    'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
    'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins',
}

DISCIPLINE_NAMES = {
    'mountain-bike': 'Mountain Bike',
    'ciclismo':      'Ciclismo',
    'triathlon':     'Triathlon',
    'trail-running': 'Trail Running',
    'audax':         'Audax',
}
VALID_DISCIPLINES = set(DISCIPLINE_NAMES)

# Duplicated in callsites — consolidate when adding new disciplines.
_DISCIPLINE_LONG_NAME = {
    'mountain-bike': 'Mountain Bike XCM e XCO',
    'ciclismo':      'Ciclismo de Estrada',
    'triathlon':     'Triathlon e Duathlon',
    'trail-running': 'Corridas de Trail',
    'audax':         'Audax e BRM Randonnée',
}

# Raw sport field values as stored in schema_events (must match DB exactly).
_SLUG_TO_SPORT = {
    'mountain-bike': 'Mountain bike',
    'ciclismo':      'Ciclismo',
    'triathlon':     'Triathlon',
    'trail-running': 'Trail running',
    'audax':         'Audax',
}

# State (UF) pages deferred to v2.
_SPORT_INTROS = {
    'mountain-bike': 'Encontre eventos e desafios de MTB em {loc}.',
    'triathlon':     'Calendário de triathlon em {loc}: sprint, olímpico e longa distância.',
    'ciclismo':      'Provas de ciclismo de estrada em {loc} para {year}.',
    'trail-running': 'Corridas de trail em {loc} em {year}.',
    'audax':         'Brevets e provas Audax/BRM em {loc}.',
}

_DISCIPLINE_CASE = """
    CASE e.sport
        WHEN 'Mountain bike' THEN 'mountain-bike'
        WHEN 'Ciclismo'      THEN 'ciclismo'
        WHEN 'Triathlon'     THEN 'triathlon'
        WHEN 'Trail running' THEN 'trail-running'
        WHEN 'Audax'         THEN 'audax'
        ELSE NULL
    END
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EVENTS_DB = Path(settings.DATA_DIR) / 'events.duckdb'


def _db(path):
    return duckdb.connect(str(path), read_only=True)


def _rows(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _organizer_name(source_url: str) -> str:
    """'https://www.ticketsports.com.br/' → 'ticketsports.com.br'"""
    if not source_url:
        return ''
    netloc = urlparse(source_url).netloc
    return netloc.removeprefix('www.')


def _fmt_date(d):
    """datetime.date → '15 Mar 2026' (locale-aware month abbrev)"""
    try:
        return d.strftime('%-d %b %Y')
    except AttributeError:
        return str(d)


def _add_date_parts(events):
    """Mutate each event dict with date_day, date_month (abbrev), date_year for templates."""
    for e in events:
        d = e.get('start_date')
        if isinstance(d, date):
            e['date_day'] = d.day
            e['date_month'] = d.strftime('%b')
            e['date_year'] = d.year
        else:
            e['date_day'] = e['date_month'] = e['date_year'] = ''
        e['organizer_name'] = _organizer_name(e.get('source', ''))


def _make_meta(events, discipline, location):
    year = date.today().year
    label = DISCIPLINE_NAMES.get(discipline, '')
    long_label = _DISCIPLINE_LONG_NAME.get(discipline, 'ciclismo e MTB')
    count = len(events)

    if label:
        title = f'Calendário {label} {location} {year} – {long_label} | RaceFeed'
    else:
        title = f'Calendário de Ciclismo em {location} {year} | RaceFeed'

    if not events:
        return title, f'Calendário de eventos de ciclismo em {location}.'

    next_ev = events[0]
    desc = (
        f'Confira {count} prova{"s" if count != 1 else ""} de {long_label} '
        f'em {location} em {year}. '
        f'Próxima: {next_ev["title"]} em {_fmt_date(next_ev["start_date"])}.'
    )
    return title, desc


def _make_editorial_intro(events, discipline, city_name, year, nearby=None):
    if not events:
        return ''
    count = len(events)
    s1 = _SPORT_INTROS.get(discipline, 'Calendário de eventos em {loc}.').format(loc=city_name, year=year)
    s2 = f'São {count} evento{"s" if count != 1 else ""} confirmados, com datas, locais e links de inscrição.'
    s3 = f'A próxima prova é {events[0]["title"]}, em {_fmt_date(events[0]["start_date"])}.'
    nearby_count = len(nearby) if nearby else 0
    s4 = f' Há também {nearby_count} evento{"s" if nearby_count != 1 else ""} em cidades próximas.' if nearby_count else ''
    return f'{s1} {s2} {s3}{s4}'


def _disc_tabs_from_events(events):
    """Discipline tabs with counts, ordered by DISCIPLINE_NAMES key order."""
    counts = Counter(e['discipline'] for e in events if e.get('discipline'))
    return [(d, DISCIPLINE_NAMES[d], counts[d]) for d in DISCIPLINE_NAMES if counts[d]]


def _load_city_events(con, city_slug):
    """All upcoming events for a city, ordered by date.
    Python slug match because city_slug column doesn't exist in schema_events (scraper gap)."""
    cur = con.execute(f"""
        SELECT
            e.title,
            e.canonical_url                  AS url,
            e.source                         AS source,
            e.date_range.start_date          AS start_date,
            e.location.city                  AS city,
            e.location.uf                    AS uf,
            {_DISCIPLINE_CASE}               AS discipline,
            g.latitude,
            g.longitude
        FROM schema_events e
        LEFT JOIN geo g
               ON g.nome = e.location.city AND g.uf = e.location.uf
        WHERE e.date_range.start_date >= current_date
          AND e.location.city IS NOT NULL
        ORDER BY e.date_range.start_date
    """)
    return [e for e in _rows(cur) if slugify(e['city']) == city_slug]


def _geo_for_city_slug(con, city_slug):
    """Return (city_name, uf, lat, lon) from geo table for a slug, or (None,)*4."""
    rows = con.execute("""
        SELECT nome, uf, latitude, longitude
        FROM geo
        WHERE strip_accents(lower(replace(nome, ' ', '-'))) = ?
        ORDER BY populacao DESC NULLS LAST
        LIMIT 10
    """, [city_slug]).fetchall()
    for nome, uf, lat, lon in rows:
        if slugify(nome) == city_slug:
            return nome, uf, lat, lon
    return None, None, None, None


def _nearby_events(con, lat, lon, city_name, discipline=None, radius_km=200, limit=20):
    """Upcoming events within radius_km of (lat, lon), excluding current city."""
    sport_clause = "AND e.sport = ?" if discipline else ""
    params = [lat, lat, lon]
    if discipline:
        params.append(_SLUG_TO_SPORT[discipline])
    params.append(city_name)

    cur = con.execute(f"""
        WITH parsed AS (
            SELECT
                e.title,
                e.canonical_url             AS url,
                e.source                    AS source,
                e.date_range.start_date     AS start_date,
                e.location.city             AS city,
                e.location.uf               AS uf,
                {_DISCIPLINE_CASE}          AS discipline,
                g.latitude,
                g.longitude,
                CAST(ROUND(6371 * 2 * asin(sqrt(
                    power(sin((radians(g.latitude)  - radians(?)) / 2), 2) +
                    cos(radians(?)) * cos(radians(g.latitude)) *
                    power(sin((radians(g.longitude) - radians(?)) / 2), 2)
                ))) AS INTEGER) AS dist_km
            FROM schema_events e
            JOIN geo g ON g.nome = e.location.city AND g.uf = e.location.uf
            WHERE e.date_range.start_date >= current_date
              AND e.location.city IS NOT NULL
              {sport_clause}
        )
        SELECT * FROM parsed
        WHERE dist_km <= {radius_km}
          AND city != ?
        QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY start_date) <= 5
        ORDER BY dist_km, start_date
        LIMIT {limit}
    """, params)
    return _rows(cur)


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

def _nearby_city_links(nearby_events):
    """Unique nearby cities ordered by proximity, with event counts."""
    seen, links = {}, []
    for e in nearby_events:
        s = slugify(e['city'])
        if s not in seen:
            seen[s] = len(links)
            links.append({'slug': s, 'nome': e['city'], 'uf': e['uf'],
                          'dist_km': e.get('dist_km'), 'count': 0})
        links[seen[s]]['count'] += 1
    return links


def city_calendar(request, city_slug, discipline=None):
    if discipline is not None and discipline not in VALID_DISCIPLINES:
        raise Http404

    with _db(_EVENTS_DB) as con:
        all_city_events = _load_city_events(con, city_slug)

        if not all_city_events:
            city_name, city_uf, lat, lon = _geo_for_city_slug(con, city_slug)
            if city_name is None:
                raise Http404
            nearby = _nearby_events(con, lat, lon, city_name, discipline=discipline) if lat else []
            events = []
        else:
            events = [e for e in all_city_events if e.get('discipline') == discipline] if discipline else all_city_events
            if discipline and not events:
                raise Http404

            lat = lon = None
            for e in all_city_events:
                if e.get('latitude') and e.get('longitude'):
                    lat, lon = e['latitude'], e['longitude']
                    break
            nearby = _nearby_events(con, lat, lon, all_city_events[0]['city'],
                                    discipline=discipline) if lat else []
            city_name = all_city_events[0]['city']
            city_uf = all_city_events[0]['uf']

    uf = city_uf.lower() if city_uf else ''
    _add_date_parts(events)
    _add_date_parts(nearby)
    title, desc = _make_meta(events, discipline, f'{city_name}, {uf.upper()}')
    canonical_path = f'/events/{city_slug}/{discipline}/' if discipline else f'/events/{city_slug}/'
    all_events = events or nearby
    year = all_events[0].get('date_year', '') if all_events else ''
    return render(request, 'tools/location_calendar.html', {
        'city_events': events,
        'nearby_events': nearby,
        'year': year,
        'city_event_count': len(events),
        'city_name': city_name,
        'state_name': UF_NAMES.get(uf.upper(), uf.upper()),
        'uf': uf,
        'city_slug': city_slug,
        'discipline': discipline,
        'discipline_name': DISCIPLINE_NAMES.get(discipline),
        'discipline_tabs': _disc_tabs_from_events(all_city_events + nearby),
        'nearby_city_links': _nearby_city_links(nearby),
        'editorial_intro': _make_editorial_intro(events, discipline, city_name, date.today().year, nearby=nearby),
        'page_title': title,
        'page_description': desc,
        'canonical': request.build_absolute_uri(canonical_path),
        'filter_uf': uf.upper(),
        'filter_city': city_name,
        'filter_discipline': _SLUG_TO_SPORT.get(discipline, ''),
    })
