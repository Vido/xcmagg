import re
import json
import time
from html import unescape
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin, urlsplit, urlunsplit, unquote_plus
from collections import OrderedDict

from bs4 import BeautifulSoup
from curl_cffi import requests as cf_requests

from bronze import Crawler, Extractor, slugify, canonical_url as generic_canonical_url


_TS_EVENT_RE = re.compile(r'(/e/)(.+)-(\d+)/?$')


def canonical_ticketsports_url(url: str) -> str:
    """TicketSports event URLs ship the same event with differing slug
    casing/encoding while sharing the trailing event id, e.g.
      .../e/super-action-indaiatuba-sunset-2026-74661
      .../e/SUPER+ACTION+INDAIATUBA+SUNSET+2026-74661
    Collapse to one canonical URL keyed by the slug-folded id.
    """
    parts = urlsplit(url)
    m = _TS_EVENT_RE.match(parts.path)
    if not m:
        return generic_canonical_url(url)  # non-event ticketsports page
    prefix, slug, event_id = m.groups()
    path = f'{prefix}{slugify(unquote_plus(slug))}-{event_id}'
    return urlunsplit((parts.scheme, parts.netloc.lower(), path, '', ''))


class BlueTicket():
    # REJECTED — generalist ticketing (rock concerts, shows, etc); sports skew
    # to bodybuilding/swimming, not the endurance/cycling core. Plus SPA ticket
    # widget (React, ~3KB shell, API-only): per-event purchase pages, no
    # crawlable calendar. Surfaces only as a registration link on other sources
    # (e.g. fetrisc). Low relevance × hard to crawl = not worth it.
    URL = 'https://site.blueticket.com.br/'
    REPO = Path('blueticket.com.br')
    META = {
        'Category': 'Empresa de Ingressos',
    }

class Semexe():
    # SHIT SOURCE — dogshit SEO content page, not a real event directory.
    # Marketplace (used bikes) bolted a static "calendario-de-eventos" timeline
    # for search traffic: link-less event cards (no <a>, no url in ld+json), no
    # detail pages, no entries sold. Data parses trivially (server-rendered +
    # ld+json ItemList) but every event url falls back to self.URL.
    URL = 'https://semexe.com/calendario-de-eventos/?estado=SP'
    REPO = Path('semexe.com')
    META = {
        'Category': 'Agregador',
        'DDD': '11',
    }

class GuiaDaCorrida():
    URL = 'https://guiadacorrida.com/'
    REPO = Path('guiadacorrida.com')
    META = {
        'Category': 'Agregador',
    }


# Local appears two ways depending on the template:
#   "Cidade/UF"   (e.g. Linhares/ES)
#   "CIDADE - UF" (e.g. SÃO MATEUS - ES)
# City = 3..50 chars (anchor uppercase + 2..49); upper bound is a runaway guard.
_SUAINSCRICAO_LOCAL_SLASH_RE = re.compile(r'([A-ZÀ-Ý][A-Za-zÀ-ÿ.\s]{2,49}?/\s*[A-Z]{2})\b')
_SUAINSCRICAO_LOCAL_DASH_RE = re.compile(r'([A-ZÀ-Ý][A-Za-zÀ-ÿ.\s]{2,49}?\s-\s[A-Z]{2})\b')


class SuaInscricao(Crawler, Extractor):
    """Registration platform (ES). Crawl list → detail.

    Listing is the open-registration page (only stable/complete one); homepage,
    /eventos and /calendario serve a rotating random ~30 of a larger pool.

    SEO-consultancy prospect: no sitemap, no robots.txt, soft-404s everything,
    randomized listings, no per-event canonical discovery. Broken SEO = lead.
    """
    URL = 'https://www.suainscricao.com/'
    REPO = Path('suainscricao.com')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Agregador',
        'DDD': '27',
        'Phone': '+55 27 996714374',
    }

    @staticmethod
    def _call(method_f, endpoint, params={}, payload={}, crawl_delay=1):
        # Site sits behind TLS fingerprinting; impersonate like the other
        # bot-gated sources (Sympla, TicketSportsAPI2).
        time.sleep(crawl_delay)
        session = cf_requests.Session(impersonate='chrome120')
        kwargs = {'params': params} if params else {}
        return session.get(endpoint, **kwargs)

    def title(self, soup) -> str:
        return soup.find('h1').get_text(strip=True)

    def date(self, soup) -> str:
        # "02/08/2026 08:00h" → "02/08/2026"; ignore time.
        m = soup.find(string=re.compile(r'\d{2}/\d{2}/\d{4}'))
        return re.search(r'\d{2}/\d{2}/\d{4}', m).group(0) if m else ''

    def local(self, soup) -> str:
        # First City/UF token on the page, either template variant.
        for regex in (_SUAINSCRICAO_LOCAL_SLASH_RE, _SUAINSCRICAO_LOCAL_DASH_RE):
            for s in soup.find_all(string=regex):
                m = regex.search(s)
                if m:
                    return ' '.join(m.group(1).split())
        return ''

    def url(self, soup) -> str:
        link = soup.find('link', rel='canonical')
        return link.get('href') if link else self._current_event_url

    def description(self, soup) -> str:
        el = soup.find('div', class_=lambda c: c and 'prose' in c and 'text-gray-600' in c)
        return el.get_text(' ', strip=True)[:500] if el else ''

    def trigger(self):
        endpoint = urljoin(self.URL, 'inscricao-aberta/provas-com-inscricao-aberta')
        fp, soup = self.get_html(endpoint, suffix='inscricao-aberta.html')

        hrefs, seen = [], set()
        for a in soup.find_all('a', href=re.compile(r'/evento/[^/]+/?$')):
            href = urljoin(self.URL, a['href'])
            if href not in seen:
                seen.add(href)
                hrefs.append(href)

        events_acc = []
        for href in hrefs:
            self._current_event_url = href
            slug = href.rstrip('/').split('/')[-1]
            try:
                _, detail = self.get_html(href, suffix=f'{slug}.html')
                events_acc.append(self.parse(detail, fp))
            except Exception as e:
                print(f'Skipping {href}: {e}')
        return events_acc


class BikeDoSul():
    URL = 'https://www.bikedosul.com.br/'
    REPO = Path('bikedosul.com.br')
    META = {
        'Category': 'Agregador',
        'DDD': '54',
    }


class AgendaEsportiva():
    URL = 'https://agendaesportiva.com.br/eventos?esporte=ciclismo'
    REPO = Path('agendaesportiva.com.br')
    META = {
        'Category': 'Agregador',
        'DDD': '41',
    }

class AgendaOffroad():
    URL = 'https://agendaoffroad.com.br/eventos?modalidade=mountain+bike'
    REPO = Path('agendaoffroad.com.br')
    META = {
        'Category': 'Agregador',
        'DDD': '41',
    }


class Fotop():
    URL = 'https://fotop.com.br'
    META = {
        'Category': 'Fotografos',
    }


class Sprinta():
    URL = 'https://www.ticketsports.com.br/Calendario/Todos-os-organizadores/Ciclismo,Mountain-bike/SP/'
    REPO = Path('sprinta.com.br')
    META = {
        'Category': 'Agregador',
        'DDD': '51',
    }


class TicketSports(Crawler, Extractor):
    """DEAD — superseded by TicketSportsAPI2. Not wired in main.py."""
    URL = 'https://www.ticketsports.com.br/'
    REPO = Path('ticketsports.com.br')
    META = {
        'Category': 'Agregador',
        'DDD': '11',
    }

    def title(self, soup) -> str:
        return soup.find('h1').text.strip()

    def date(self, soup) -> str:
        span = soup.find_all('span')
        return span[1].text.strip() 

    def local(self, soup) -> str:
        span = soup.find_all('span')
        return span[2].text.strip() 

    def url(self, soup) -> str:
        link = soup.find('link', rel="canonical")
        return link.get('href')

    def trigger(self):
        endpoint = urljoin(self.URL, 'Calendario/Todos-os-organizadores/Ciclismo,Mountain-bike/Todo-o-Brasil/Todas-as-cidades/0,00/0,00/false/?termo=&periodo=0&mes=&inicio=&fim=&ordenacao=3&pais=')
        fp, soup = self.get_html(endpoint, suffix='calendario')
        div = soup.find_all('div', 'card-evento')

        href_list = []
        for d in div:
            href = d.find('a').get('href')
            href_list.append(href)

        events_acc = []
        for href in href_list:
            url = urljoin(self.URL, href)
            fn = re.sub(r'(?u)[^-\w.]', '_', href)
            fp, soup2 = self.get_html(url, suffix=fn)
            events_acc.append(self.parse(soup2, fp))

        return events_acc


class TicketSportsAPI(Crawler, Extractor):
    """DEAD — superseded by TicketSportsAPI2. Not wired in main.py."""
    URL = 'https://www.ticketsports.com.br/'
    REPO = Path('api.ticketsports.com.br')

    @staticmethod
    def _call(method_f, endpoint, params={}, payload={}, crawl_delay=1):
        time.sleep(crawl_delay)
        session = cf_requests.Session(impersonate='chrome120')
        kwargs = {}
        if params:
            kwargs['params'] = params
        if payload:
            kwargs['data'] = json.dumps(payload)
        return getattr(session, method_f.__name__)(endpoint, **kwargs)

    META = {
        'Category': 'Agregador',
        'DDD': '11',
    }

    def title(self, data) -> str:
        return data['Titulo']

    def date(self, data) -> str:
        return data['DataRealizacaoString']

    def local(self, data) -> str:
        return data['UF'] + ' / ' + data['Cidade']

    def url(self, data) -> str:
        from urllib.parse import unquote_plus
        title_slug = unquote_plus(data["TituloUrl"]).replace(' ', '-').lower()
        return urljoin(self.URL, f'e/{title_slug}-{data["IdEvento"]}')

    def trigger(self):

        payload = lambda: {
            'organizador':'Todos-os-organizadores',
            'termo':'',
            'uf':'Todo-o-Brasil',
            'cidade':'Todas-as-cidades',
            'periodo':0,
            'mes':'',
            'inicio':'',
            'fim':'',
            'filtroRapido':'Ciclismo,Mountain-bike',
            'ids': ','.join([f'{k}' for k in (ids_set.keys())]),
            'apenasInscricoesAbertas':'true',
            'precoDe':'',
            'precoAte':'',
            'freteGratis':'false',
            'ordenacao':1,
            'pais':'Brasil',
        }

        page = 1
        events_acc = []
        ids_set = OrderedDict()
        api = urljoin(self.URL, 'Calendario')

        while True:
            fp, data = self.get_json(api, suffix=f'calendario{page}.json', payload=payload())
            if not data:
                break
            events_acc += [self.parse(row, fp) for row in data]
            ids_set |= {obj['IdEvento']:'' for obj in data if 'IdEvento' in obj}
            page += 1

        return events_acc


class TicketSportsAPI2(Crawler, Extractor):
    URL = 'https://www.ticketsports.com.br/'
    REPO = Path('api.ticketsports.com.br')

    @staticmethod
    def _call(method_f, endpoint, params={}, payload={}, crawl_delay=1):
        time.sleep(crawl_delay)
        session = cf_requests.Session(impersonate='chrome120')
        kwargs = {'params': params} if params else {}
        return session.get(endpoint, **kwargs)

    META = {
        'Category': 'Agregador',
        'DDD': '11',
    }

    def title(self, data) -> str:
        return data['title']

    def date(self, data) -> str:
        return data['date']

    def local(self, data) -> str:
        return data['address']

    def url(self, data) -> str:
        return data['uri']

    def canonical_url(self, url: str) -> str:
        return canonical_ticketsports_url(url)

    QUICK_FILTERS = ['mountain-bike', 'ciclismo', 'triathlon']

    def trigger(self):
        QUANTITY = 200
        events_acc = []
        seen = set()
        api = urljoin(self.URL, 'api/events/list')

        for qf in self.QUICK_FILTERS:
            page = 1
            while True:
                params = {
                    'quantity': QUANTITY,
                    'atlheteId': 0,
                    'quickFilter': qf,
                    'country': 'BR',
                    'page': page,
                }
                fp, data = self.get_json(api, suffix=f'events-{qf}-{page}.json', params=params)
                if not data:
                    break
                for row in data:
                    if row['eventId'] not in seen:
                        seen.add(row['eventId'])
                        events_acc.append(self.parse(row, fp))
                if len(data) < QUANTITY:
                    break
                page += 1

        return events_acc



class RaizesEsportes(Crawler, Extractor):
    """Tour da Roça registration platform (checkout.raizesesportes.com.br).
    Homepage lists all open-registration events as cards. Server-rendered."""
    URL = 'https://checkout.raizesesportes.com.br/'
    REPO = Path('checkout.raizesesportes.com.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Organizador',
    }

    def title(self, card) -> str:
        return card.find('p', class_=lambda c: c and 'font-semibold' in c).get_text(strip=True)

    def date(self, card) -> str:
        badge = card.find('div', class_=lambda c: c and 'absolute' in c and 'top-4' in c)
        return badge.get_text(strip=True) if badge else ''

    def local(self, card) -> str:
        loc = card.find('p', class_=lambda c: c and 'text-indigo-600' in c)
        return loc.get_text(strip=True) if loc else ''

    def url(self, card) -> str:
        a = card.find('a', href=True)
        return a['href'] if a else self.URL

    def trigger(self):
        fp, soup = self.get_html(self.URL, suffix='index.html')
        cards = soup.find_all('div', class_=lambda c: c and 'rounded-lg' in c and 'shadow-lg' in c)
        events = []
        for card in cards:
            try:
                events.append(self.parse(card, fp))
            except Exception as e:
                print(f'Skipping card: {e}')
        return events


class TourDoPeixe(Crawler, Extractor):
    URL = 'https://tourdopeixe.com.br'
    REPO = Path('tourdopeixe.com.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Organizador',
        'DDD': '11',
    }

    def title(self, soup) -> str:
        return soup.find_all('td')[1].text.strip()
    
    def date(self, soup) -> str:
        return soup.find_all('td')[0].text.strip()

    def local(self, soup) -> str:
        return soup.find_all('td')[3].text.strip()
    
    def url(self, soup) -> str:
        if not soup.find('a'):
            return self.URL
        return soup.find('a').get('href')

    def trigger(self):
        endpoint = self.URL + '/calendario/'
        fp, soup = self.get_html(endpoint, suffix='calendario')
        tr = soup.find('table').find_all('tr')

        events_acc = []
        for i, t in enumerate(tr):
            if not i:
                continue # header
            try:
                events_acc.append(self.parse(t, fp))
            except ValueError as ve:
                continue

        return events_acc


class InscricoesBike(Crawler, Extractor):
    """Dead: inscricoes.bike rebranded to inscricoes.com.br, this static JSON
    API is NXDOMAIN. Site's catalog is already fully covered by
    InscricoesBr (same domain, no gap to fill)."""

    URL = 'https://inscricoes.bike/'
    REPO = Path('inscricoes.bike')
    META = {
        'Category': 'Agregador',
        'DDD': '79',
    }

    def title(self, soup) -> str:
        return soup['titulo']

    def date(self, soup) -> str:
        return soup['dataevento']

    def local(self, soup) -> str:
        return soup['cidade'] + '-' + soup['uf']

    def url(self, soup) -> str:
        return urljoin(self.URL, soup['url'])

    def trigger(self):
        api = 'https://static.inscricoes.bike/eventos/eventos-bike.json'
        fp, data = self.get_json(api, suffix='eventos.json')

        events_acc = []
        for row in data:
            events_acc.append(self.parse(row, fp))

        return events_acc


class Atletis(Crawler, Extractor):
    URL = 'https://www.atletis.com.br/'
    REPO = Path('atletis.com.br')
    META = {
        'Category': 'Agregador',
    }

    def title(self, soup) -> str:
        return soup['data-name'].strip()

    def date(self, soup) -> str:
        return soup['data-date'].strip()

    def local(self, soup) -> str:
        infos = soup.find('div', class_='event-card').find_all('div', class_='event-card-info')
        if len(infos) >= 2:
            return infos[1].get_text(strip=True)
        return ''

    def url(self, soup) -> str:
        return soup['data-url']

    def sport(self, soup) -> str:
        script = soup.find('script', type='application/ld+json')
        if not script:
            return ''
        _sport = json.loads(script.string).get('sport', '')
        return _sport

    @staticmethod
    def _parse_date(raw: str):
        import locale
        from datetime import datetime
        locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
        try:
            return datetime.strptime(raw.strip(), "%d %B %Y").date()
        except ValueError:
            return None

    def trigger(self):
        from datetime import date as date_t
        events = []
        page = 1
        while True:
            endpoint = (
                urljoin(self.URL, 'eventos') if page == 1
                else urljoin(self.URL, f'eventos/{page}')
            )
            fp, soup = self.get_html(endpoint, suffix=f'eventos-p{page}.html')
            event_divs = soup.find_all('div', attrs={'data-event': True})
            if not event_divs:
                break

            future_divs = [
                div for div in event_divs
                if (d := self._parse_date(div['data-date'])) and d >= date_t.today()
            ]
            if not future_divs:
                break

            for div in future_divs:
                event_url = div['data-url']
                slug = event_url.rstrip('/').split('/')[-1]
                fp2, detail = self.get_html(event_url, suffix=f'{slug}.html')
                event = self.parse(div, fp)
                event.sport = self.sport(detail)
                events.append(event)

            if not soup.find('a', class_='pagination-next'):
                break
            page += 1
        return events


class InscricoesBr(Crawler, Extractor):
    URL = 'https://inscricoes.com.br/'
    REPO = Path('inscricoes.com.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Agregador',
    }

    def title(self, card) -> str:
        return card.find('h3').get_text(strip=True)

    def date(self, card) -> str:
        icon = card.find('i', attrs={'data-lucide': 'calendar'})
        return icon.parent.find('span').get_text(strip=True) if icon else ''

    def local(self, card) -> str:
        icon = card.find('i', attrs={'data-lucide': 'map-pin'})
        return icon.parent.find('span').get_text(strip=True) if icon else ''

    def url(self, card) -> str:
        return card['href']

    def trigger(self):
        fp, soup = self.get_html(urljoin(self.URL, 'eventos'), suffix='eventos.html')

        events_acc = []
        for card in soup.find_all('a', class_='event-item'):
            events_acc.append(self.parse(card, fp))
        return events_acc


# Local lives in the event description prose, two phrasings:
#   "acontece em Casa Branca (MG)"  /  "em Caraguatatuba/SP"
# Anchored-on-"em" form tried first so the city capture doesn't swallow a
# capitalized sentence prefix; bare form is the fallback.
_PP_CITY = r'([A-ZÀ-Ý][A-Za-zÀ-ÿ.\s-]{1,49}?)\s*(?:\(([A-Z]{2})\)|/([A-Z]{2})\b)'
_PP_LOCAL_ANCHORED_RE = re.compile(r'\bem\s+' + _PP_CITY)
_PP_LOCAL_RE = re.compile(_PP_CITY)


class ProximaProva(Crawler, Extractor):
    """MTB-focused calendar aggregator. WordPress + The Events Calendar:
    events come from the tribe REST API (title/dates/url/description — venue
    is always empty, local is parsed out of the description). Sport comes
    from the custom pp_modalidade taxonomy, only exposed on the wp/v2
    endpoint, joined back to tribe events by event link.

    DISABLED (2026-07-16) — site is broken: every /evento/ detail page (and
    the /eventos/ listing) returns HTTP 500 after ~20s (PHP fatal), only the
    homepage + REST APIs respond. url=PK dereferences to a dead page, and the
    `website` fallback field is a non-unique organizer homepage (23 unique
    across 40 events — CBC disease). Crawler kept implemented + working
    against the REST API; re-enable in main.py if they fix their templates.
    Meanwhile the organizer sites it aggregates are stubbed below."""
    URL = 'https://proximaprova.com.br/'
    REPO = Path('proximaprova.com.br')
    META = {
        'Category': 'Agregador',
    }

    EVENTS_API = 'https://proximaprova.com.br/wp-json/tribe/events/v1/events'
    WP2_API = 'https://proximaprova.com.br/wp-json/wp/v2/tribe_events'
    MODALIDADE_API = 'https://proximaprova.com.br/wp-json/wp/v2/pp_modalidade'

    # pp_modalidade term name -> canonical sport (agents.SPORTS values);
    # unmapped/absent terms fall back to '' = LLM classification downstream.
    MODALIDADE_SPORT = {
        'Mountain Bike': 'Mountain bike',
        'Estrada': 'Ciclismo',
        'Gravel': 'Ciclismo',
        'Cicloturismo': 'Ciclismo',
        'Corrida': 'Corrida de Rua',
        'Trail': 'Trail running',
    }

    def title(self, data) -> str:
        return unescape(data['title']).strip()

    def date(self, data) -> str:
        start = data['start_date'][:10]
        end = (data.get('end_date') or '')[:10]
        return f'{start} a {end}' if end and end != start else start

    def _desc_text(self, data) -> str:
        return BeautifulSoup(data.get('description', ''), 'lxml').get_text(' ', strip=True)

    def local(self, data) -> str:
        text = self._desc_text(data)
        m = _PP_LOCAL_ANCHORED_RE.search(text) or _PP_LOCAL_RE.search(text)
        if not m:
            return ''
        uf = m.group(2) or m.group(3)
        return f'{m.group(1).strip()}/{uf}'

    def description(self, data) -> str:
        return self._desc_text(data)

    def url(self, data) -> str:
        return data['url']

    def sport(self, data) -> str:
        return self._sport_by_link.get(data['url'], '')

    def _sport_lookup(self) -> dict:
        _, terms = self.get_json(
            self.MODALIDADE_API, suffix='modalidades.json',
            params={'per_page': 100})
        names = {t['id']: t['name'] for t in terms}

        lookup, page = {}, 1
        while True:
            _, rows = self.get_json(
                self.WP2_API, suffix=f'wp2-{page}.json',
                params={'per_page': 100, 'page': page,
                        '_fields': 'link,pp_modalidade'})
            for row in rows:
                mods = (names.get(i, '') for i in row.get('pp_modalidade', []))
                sport = next(
                    (self.MODALIDADE_SPORT[m] for m in mods
                     if m in self.MODALIDADE_SPORT), '')
                lookup[row['link']] = sport
            if len(rows) < 100:
                break
            page += 1
        return lookup

    def trigger(self):
        self._sport_by_link = self._sport_lookup()

        events_acc, page = [], 1
        while True:
            fp, data = self.get_json(
                self.EVENTS_API, suffix=f'tribe-{page}.json',
                params={'per_page': 50, 'page': page})
            for row in data.get('events', []):
                events_acc.append(self.parse(row, fp))
            if page >= data.get('total_pages', 1):
                break
            page += 1
        return events_acc
