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
    'mountain-bike': 'Encontre trilhas e provas de MTB em {loc}.',
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

def _db():
    path = Path(settings.DATA_DIR) / 'events.duckdb'
    return duckdb.connect(str(path), read_only=True)


def _rows(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


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


def _make_meta(events, discipline, location):
    year = date.today().year
    label = DISCIPLINE_NAMES.get(discipline, '')
    count = len(events)

    if label and count:
        title = f'{count} Provas de {label} em {location} — {year} | RaceFeed'
    else:
        title = f'Calendário de Ciclismo e MTB em {location} {year} | RaceFeed'

    if not events:
        return title, f'Calendário de eventos de ciclismo em {location}.'

    next_ev = events[0]
    intro = _SPORT_INTROS.get(discipline, 'Calendário de eventos em {loc}.').format(
        loc=location, year=year,
    )
    cities = list(dict.fromkeys(e['city'] for e in events if e.get('city')))[:3]
    desc = (
        f'{intro} {count} evento{"s" if count != 1 else ""} em {year}. '
        f'Próximo: {next_ev["title"]} em {_fmt_date(next_ev["start_date"])}. '
        f'Cidades: {", ".join(cities)}.'
    )
    return title, desc


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


def city_calendar(request, city_slug):
    with _db() as con:
        events = _load_city_events(con, city_slug)
        if not events:
            raise Http404

        lat = lon = None
        for e in events:
            if e.get('latitude') and e.get('longitude'):
                lat, lon = e['latitude'], e['longitude']
                break
        nearby = _nearby_events(con, lat, lon, events[0]['city']) if lat else []

    city_name = events[0]['city']
    uf = events[0]['uf'].lower()
    combined = sorted(events + nearby, key=lambda e: e['start_date'])
    ssr = combined
    _add_date_parts(ssr)
    title, desc = _make_meta(events, None, f'{city_name}, {uf.upper()}')
    return render(request, 'tools/location_calendar.html', {
        'ssr_events': ssr,
        'city_event_count': len(events),
        'total_count': len(combined),
        'city_name': city_name,
        'state_name': UF_NAMES.get(uf.upper(), uf.upper()),
        'uf': uf,
        'city_slug': city_slug,
        'discipline': None,
        'discipline_name': None,
        'discipline_tabs': _disc_tabs_from_events(combined),
        'nearby_city_links': _nearby_city_links(nearby),
        'page_title': title,
        'page_description': desc,
        'canonical': request.build_absolute_uri(f'/events/{city_slug}/'),
        'filter_uf': uf.upper(),
        'filter_city': city_name,
        'filter_discipline': '',
    })


def city_discipline_calendar(request, city_slug, discipline):
    if discipline not in VALID_DISCIPLINES:
        raise Http404

    with _db() as con:
        all_city_events = _load_city_events(con, city_slug)

        events = [e for e in all_city_events if e.get('discipline') == discipline]
        if not events:
            raise Http404

        lat = lon = None
        for e in all_city_events:
            if e.get('latitude') and e.get('longitude'):
                lat, lon = e['latitude'], e['longitude']
                break
        nearby = _nearby_events(con, lat, lon, all_city_events[0]['city'],
                                discipline=discipline) if lat else []

    city_name = events[0]['city']
    uf = events[0]['uf'].lower()
    combined = sorted(events + nearby, key=lambda e: e['start_date'])
    ssr = combined
    _add_date_parts(ssr)
    title, desc = _make_meta(events, discipline, f'{city_name}, {uf.upper()}')
    return render(request, 'tools/location_calendar.html', {
        'ssr_events': ssr,
        'city_event_count': len(events),
        'total_count': len(combined),
        'city_name': city_name,
        'state_name': UF_NAMES.get(uf.upper(), uf.upper()),
        'uf': uf,
        'city_slug': city_slug,
        'discipline': discipline,
        'discipline_name': DISCIPLINE_NAMES[discipline],
        'discipline_tabs': _disc_tabs_from_events(all_city_events + nearby),
        'nearby_city_links': _nearby_city_links(nearby),
        'page_title': title,
        'page_description': desc,
        'canonical': request.build_absolute_uri(f'/events/{city_slug}/{discipline}/'),
        'filter_uf': uf.upper(),
        'filter_city': city_name,
        'filter_discipline': _SLUG_TO_SPORT[discipline],
    })
