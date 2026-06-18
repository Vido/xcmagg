import re
import json
import time
from pathlib import Path
from urllib.parse import urljoin

from curl_cffi import requests as cf_requests

from bronze import Crawler, Extractor, slugify


_EVENT_PATH_RE = re.compile(r'/evento/[a-z0-9\-]+/(\d+)')


class Sympla(Crawler, Extractor):
    """Generalist ticketing platform (Belo Horizonte). Mostly non-sport events
    (shows/parties), so we discover via sport-scoped keyword search and gate to
    the ESPORTIVO vertical before emitting. Surviving events keep sport='' for
    the Silver LLM to classify into the project's canonical sports.
    """
    URL = 'https://www.sympla.com.br/'
    REPO = Path('sympla.com.br')
    META = {'Category': 'Agregador', 'DDD': '31'}

    # Server-rendered listing requires ?s= (bare /eventos/esportes is client-side
    # and ships no links). Keywords bias the search toward the endurance space.
    KEYWORDS = ['mountain bike', 'ciclismo', 'mtb', 'gravel', 'pedal', 'bike']
    MAX_PAGES = 5

    @staticmethod
    def _call(method_f, endpoint, params={}, payload={}, crawl_delay=1):
        # Sympla blocks non-impersonated TLS; use curl_cffi like the other
        # bot-gated sources (CorridaPronta, TicketSportsAPI2).
        time.sleep(crawl_delay)
        session = cf_requests.Session(impersonate='chrome120')
        kwargs = {'params': params} if params else {}
        return session.get(endpoint, **kwargs)

    def title(self, d) -> str:
        return d['name'].strip()

    def date(self, d) -> str:
        return d['startDate']

    def url(self, d) -> str:
        return urljoin(self.URL, f'evento/{d["slug"]}/{d["id"]}')

    def local(self, d) -> str:
        addr = d.get('eventsAddress') or {}
        city, state = addr.get('city'), addr.get('state')
        if city and state:
            return f'{city} - {state}'
        return addr.get('name') or 'Online'

    @staticmethod
    def _event_dict(soup) -> dict:
        tag = soup.find('script', id='__NEXT_DATA__')
        data = json.loads(tag.string)
        return data['props']['pageProps']['hydrationData']['eventHydration']['event']

    @staticmethod
    def _is_sport(event: dict) -> bool:
        # The ESPORTIVO vertical also bundles wellness sub-categories
        # (saude-nutricao-e-bemestar: yoga, courses, ...), so gate on the
        # 'esportes' sub-category specifically.
        if event.get('cancelled') or not event.get('visible', True):
            return False
        category = event.get('eventsCategory') or {}
        return category.get('name') == 'esportes'

    def _search_ids(self) -> list:
        """Discover unique event (id, path) pairs across keyword searches.

        Result links live in the Next.js RSC payload (inline <script> JSON), not
        as real <a> anchors, so match them on the raw HTML text.
        """
        seen, found = set(), []
        for kw in self.KEYWORDS:
            kw_slug = slugify(kw)
            for page in range(1, self.MAX_PAGES + 1):
                endpoint = urljoin(self.URL, f'eventos/esportes?s={kw}&page={page}')
                fp, _ = self.get_html(endpoint, suffix=f'search-{kw_slug}-{page}.html')
                html = fp.read_text(encoding='utf-8', errors='ignore')
                new_on_page = 0
                for m in _EVENT_PATH_RE.finditer(html):
                    path, event_id = m.group(0), m.group(1)
                    if event_id not in seen:
                        seen.add(event_id)
                        found.append((event_id, path))
                        new_on_page += 1
                if not new_on_page:
                    break  # exhausted this keyword
        return found

    def trigger(self):
        events_acc = []
        for event_id, href in self._search_ids():
            url = urljoin(self.URL, href)
            fp, soup = self.get_html(url, suffix=f'{event_id}.html')
            try:
                event = self._event_dict(soup)
            except Exception as e:
                print(f'Skipping {href}: {e}')
                continue
            if not self._is_sport(event):
                continue
            try:
                events_acc.append(self.parse(event, fp))
            except Exception as e:
                print(f'Skipping {href}: {e}')
        return events_acc
