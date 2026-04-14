import re
import unicodedata
from pathlib import Path
from datetime import datetime, date
from urllib.parse import urlparse, urljoin

from curl_cffi import requests as cf_requests
from bronze import Crawler, Extractor


class TIOnline(Crawler, Extractor):
    URL = 'https://tionline.net.br/'
    REPO = Path('tionline.net.br')
    TIME_FORMAT = '%d/%m/%Y - %H:%M'
    META = {
        'Category': 'Empresa de Cronometragem',
        'DDD': '38',
        'Tags': ['Kenda Cup',]
    }

    def title(self, soup) -> str:
        return soup.find('div', class_='title').text.strip()
    
    def date(self, soup) -> str:
        return soup.find('div', class_='hours').text.strip()

    def local(self, soup) -> str:
        return soup.find('div', class_='local').text.strip()
    
    def url(self, soup) -> str:
        return soup.find('a').get('href')

    def trigger(self):
        fp, soup = self.get_html(self.URL, suffix='home.html')
        div = soup.find_all('div', 'slider__footer')
        events_acc = []
        for d in div:
            events_acc.append(self.parse(d, fp))

        return events_acc


class CorridaPronta(Crawler, Extractor):
    URL = 'https://www.corridapronta.com.br/'
    REPO = Path('corridapronta.com.br')
    META = {
        'Category': 'Empresa de Cronometragem',
        'Tags': ['FBR Esportes',]
    }


    def _call(self, method_f, endpoint, params={}, payload={}, crawl_delay=1):
        import time
        time.sleep(crawl_delay)
        cf_method = cf_requests.post if method_f.__name__ == 'post' else cf_requests.get
        kwargs = {'impersonate': 'chrome'}
        if params:
            kwargs['params'] = params
        if payload:
            kwargs['data'] = payload
        return cf_method(endpoint, **kwargs)

    def title(self, soup) -> str:
        p = soup.find('p')
        return p.contents[0].strip()

    def date(self, soup) -> str:
        return soup.find('span', class_='local').text.strip()
        
    def local(self, soup) -> str:
        span = soup.find_all('span', class_='local')
        return span[1].text.strip()

    def url(self, soup) -> str:
        return urljoin(self.URL, 'inscricao/', soup.find('a').get('href'))

    def trigger(self):
        endpoint = urljoin(self.URL, 'eventos.php')
        fp, soup = self.get_html(endpoint, suffix='eventos')
        div = soup.find_all('div', 's-12 m-6 l-3')

        href_list = []
        for d in div:
            href = d.find('a').get('href')
            if href != 'http://www.fbresportes.com':
                href_list.append(href)

        events_acc = []
        for href in href_list:
            url = urljoin(self.URL, href)
            fn = re.sub(r'(?u)[^-\w.]', '_', href)
            fp, soup2 = self.get_html(url, suffix=fn)
            events_acc.append(self.parse(soup2, fp))
        return events_acc


class Corridao(Crawler, Extractor):
    URL = 'https://www.corridao.com.br/'
    REPO = Path('corridao.com.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Empresa de Cronometragem',
        'Tags': ['Corrida de Rua', 'MG', 'RJ'],
    }


class ActiveSports(Crawler, Extractor):
    URL = 'https://www.activesports.com.br/'
    REPO = Path('activesports.com.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Empresa de Cronometragem',
        'Tags': ['Rota do Vulcão XCM',],
        'DDD': '35',
    }

    def title(self, soup) -> str:
        return soup[0].text.strip()
    
    def date(self, soup) -> str:
        return soup[2].text.strip()

    def local(self, soup) -> str:
        return soup[1].text.strip()
    
    def url(self, soup) -> str:
        return urljoin(self.URL, soup[0].get('href'))

    def trigger(self):
        endpoint = urljoin(self.URL, 'proximos-eventos')
        fp, soup = self.get_html(endpoint, suffix='proximos-eventos')
        div = soup.find_all('div', class_='content-course')

        events_acc = []
        for d in div:
            events_acc.append(self.parse(d.find_all('a'), fp))

        return events_acc

class InscricaoExtreme(Crawler, Extractor):
    URL = 'https://www.inscricoesxtreme.com.br/'
    PATH = Path('inscricoesxtreme.com.br')
    META = {
        'Category': 'Empresa de Cronometragem',
        'Tags': ['Corrida de Rua', 'SP'],
    }


class GLPromo:
    URL = 'https://www.glpromo.com.br/eventos-esportivos-tipo/bike/4'
    REPO = Path('glpromo.com.br')
    META = {
        'Category': 'Empresa de Organizadora',
        'DDD': '43',
    }


class GpsControlCrono(Crawler, Extractor):
    URL = 'https://www.gpscontrolcrono.com.br/'
    REPO = Path('gpscontrolcrono.com.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Empresa de Cronometragem',
        'Tags': ['Desafio do Ventos', 'MG', 'ES'],
        'DDD': '35',
    }

    def title(self, soup) -> str:
        # Title is the <font size="4"> inside the second <tr>
        title_font = soup.find('font', size='4')
        return title_font.get_text(strip=True)

    def _info_line(self, soup) -> str:
        for p in soup.find_all('p', align='center'):
            text = p.get_text(strip=True)
            if re.match(r'.+ - [A-Z]{2} - \d{2}/\d{2}/\d{4}', text):
                return text
        return ''

    def local(self, soup) -> str:
        text = self._info_line(soup)
        # "Extrema - MG - 08/02/2026"
        parts = [p.strip() for p in text.split('-')]
        return ' - '.join(parts[:2]) if len(parts) >= 2 else ''

    def date(self, soup) -> str:
        text = self._info_line(soup)
        parts = [p.strip() for p in text.split('-')]
        return parts[-1] if len(parts) >= 3 else ''

    def url(self, soup) -> str:
        return self._current_event_url

    def trigger(self):
        fp, soup = self.get_html(self.URL, suffix='home.html')
        seen, hrefs = set(), []
        for a in soup.find_all('a', href=re.compile(r'evento\d+\.php')):
            href = a['href']
            if href not in seen:
                seen.add(href)
                hrefs.append(href)

        events_acc = []
        for href in hrefs:
            event_id = re.search(r'evento(\d+)', href).group(1)
            self._current_event_url = urljoin(self.URL, href)
            efp, event_soup = self.get_html(self._current_event_url, suffix=f'evento{event_id}.html')
            events_acc.append(self.parse(event_soup, efp))

        return events_acc


class SeuEsporteApp(Crawler, Extractor):
    URL = 'https://inscricao.seuesporte.app/'
    REPO = Path('seuesporte.app')

    def title(self, soup) -> str:
        tag = soup.find_all('a')
        return tag[1].text.strip()

    def date(self, soup) -> str:
        tag = soup.find('p').text.split('\n')
        return tag[2].strip()

    def local(self, soup) -> str:
        tag = soup.find('p').text.split('\n')
        return tag[1].strip()

    def url(self, soup) -> str:
        tag = soup.find_all('a')
        return tag[1].get('href')

    def trigger(self):
        fp, soup = self.get_html(self.URL)
        div = soup.find_all('div', class_='block block-rounded h-100 mb-0')

        events_acc = []
        for d in div:
            events_acc.append(self.parse(d, fp))
        return events_acc


class Peloto(Crawler, Extractor):
    URL = 'https://peloto.com.br/'
    REPO = Path('peloto.com.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Empresa de Cronometragem',
        'Tags': ['Copa Regional de MTB',],
        'DDD': '16',
    }

    def title(self, soup) -> str:
        div = soup.find('div', class_='row red darken-4 white-text center')
        if div:
            return div.find('h4').text.strip()
        # new LP-style page: use og:title meta tag
        meta = soup.find('meta', property='og:title')
        if meta:
            return meta.get('content', '').strip()
        return soup.find('title').text.strip()

    def date(self, soup) -> str:
        div = soup.find('div', class_='row red darken-4 white-text center')
        if div:
            return div.find_all('h5')[1].text.strip()
        # new LP-style page: og:description is "CITY - DD/MM/YYYY"
        meta = soup.find('meta', property='og:description')
        if meta:
            content = meta.get('content', '')
            parts = content.split(' - ')
            return parts[-1].strip()
        raise ValueError("Cannot extract date from page")

    def local(self, soup) -> str:
        local = soup.find('div', class_='col s12 m8 l8 white-text')
        if local:
            return local.find('div', class_='card').find('h5').text.strip()
        # new LP-style page: og:description is "CITY - DD/MM/YYYY"
        meta = soup.find('meta', property='og:description')
        if meta:
            content = meta.get('content', '')
            parts = content.split(' - ')
            return parts[0].strip()
        raise ValueError("Cannot extract local from page")

    def url(self, soup) -> str:
        href_tag = soup.find('a', class_='btn-large')
        if href_tag:
            href = href_tag.get('href')
            return urljoin(self.URL, href)
        # new LP-style page: use og:url
        meta = soup.find('meta', property='og:url')
        if meta:
            return meta.get('content', '').strip()
        raise ValueError("Cannot extract url from page")

    def trigger(self):
        fp, soup = self.get_html(self.URL, suffix='home.html')
        div = soup.find_all('div', 'prox-eventos')

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


class ProximaProva():
    URL = 'https://proximaprova.com.br/eventos/'


class Nuflow(Crawler, Extractor):
    URL = 'https://nuflowpass.com.br/'
    REPO = Path('nuflowpass.com.br')
    META = {
        'Category': 'Empresa de Cronometragem',
        'DDD': '31',
    }
    def title(self, data):
        return data['name']

    def date(self, data):
        return data['formatted_date']

    def local(self, data):
        return data['full_address']

    def url(self, data):
        event_id = data['id']
        return urljoin(self.URL, f'events/{event_id}')

    def trigger(self):
        api = 'https://nuflowpass.herokuapp.com/api/v2/events'
        fp, data = self.get_json(api, suffix='events.json')

        events_acc = []
        for row in data['events']:
            events_acc.append(self.parse(row, fp))

        return events_acc

class TicketBr(Crawler, Extractor):
    URL = 'https://www.ticketbr.com.br/'
    REPO = Path('ticketbr.com.br')
    TIME_FORMAT = '%d/%m/%Y %H:%M'
    META = {
        'Category': 'Empresa de Ingressos',
        'DDD': '16',
    }
    def title(self, soup) -> str:
        raw = soup.find('h5').text.strip()
        # Strip "Evento: DD/MM/YYYY " artifact from TicketBR HTML
        return re.sub(r'^Evento:\s*(?:\d{2}/\d{2}/\d{4}\s*-?\s*)?', '', raw).strip()
    
    def date(self, soup) -> str:
        try:
            return soup.find('h4').text.strip()
        except AttributeError:
            return soup.find('h5').text.strip()
        
    def local(self, soup) -> str:
        local = soup.find('div', class_='cidade').text.strip()
        return local.replace('Cidade: ', '')

    def sport(self, soup) -> str:
        tag = soup.find('div', class_='modalidade')
        if not tag:
            return ''
        return tag.get_text(strip=True).replace('Modalidade:', '').strip()

    def url(self, soup) -> str:
        pk = soup.find('div', class_='inscricao').find('a')
        if not pk:
            return self.URL
        pk = ''.join(filter(str.isdigit, pk.get('onclick')))
        url = urljoin(self.URL, f'/evento/undefined/{pk}')
        return url

    def trigger(self):
        from bs4 import BeautifulSoup
        endpoint = urljoin(self.URL, '/calendario.ecm')
        fp, soup = self.get_html(endpoint, suffix='calendario', encoding='iso-8859-1')
        div = soup.find('div', class_='calendario')

        buffer, events_acc = '', []
        for node in div.children:
            if node.name == 'hr':
                new_soup = BeautifulSoup(buffer, "lxml")
                event = self.parse(new_soup, fp)
                events_acc.append(event)
                buffer = '' # Reset State
            else:
                if not node.name and str(node).strip():
                    continue
                buffer += str(node)

        return events_acc

class Polesportivo(Crawler, Extractor):
    URL = 'https://polesportivo.com.br/'
    REPO = Path('polesportivo.com.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Empresa de Cronometragem',
        'DDD': '11',
    }

    def title(self, soup) -> str:
        return soup.find('h2', class_='tituloEvento').text.strip()

    def date(self, soup) -> str:
        h3 = soup.find('h3', class_='mdc-typography--body1')
        # text is "event\xa0DD/MM/YYYY" — strip the material-icon name
        text = h3.get_text(strip=True)
        return re.sub(r'^event\s*', '', text)

    def local(self, soup) -> str:
        return soup.find('div', class_='cidadeEvento').get_text(strip=True)

    def url(self, soup) -> str:
        btn = soup.find('button', class_='inscreva')
        if btn:
            onclick = btn.get('onclick', '')
            m = re.search(r"window\.open\('([^']+)'", onclick)
            if m:
                return m.group(1)
        # fallback: event detail page
        action = soup.find('div', class_='grid-card__primary-action')
        onclick = action.get('onclick', '')
        m = re.search(r"window\.open\('([^']+)'", onclick)
        return urljoin(self.URL, m.group(1)) if m else self.URL

    def trigger(self):
        endpoint = urljoin(self.URL, 'eventos.php')
        fp, soup = self.get_html(endpoint, suffix='eventos.html')
        cells = soup.find_all('div', class_='mdc-layout-grid__cell')

        events_acc = []
        for cell in cells:
            events_acc.append(self.parse(cell, fp))
        return events_acc


class _DesafioRuralBase(Crawler, Extractor):
    META = {
        'Category': 'Organizador',
        'DDD': '11',
        'Tags': ['MTB', 'Trail Run'],
    }
    _CPF  = '652.865.660-65'
    _NASC = '09/07/1990'

    def _well(self, soup):
        for div in soup.find_all('div', class_='well'):
            if div.find('b'):
                return div
        return None

    def title(self, soup) -> str:
        return self._well(soup).find_all('h3')[2].get_text(strip=True)

    def date(self, soup) -> str:
        return self._well(soup).find_all('h3')[1].get_text(strip=True)

    def local(self, soup) -> str:
        return self._well(soup).find_all('h3')[0].get_text(strip=True)

    def url(self, soup) -> str:
        a = self._well(soup).find('a', onclick=re.compile(r'Inscrever'))
        m = re.search(r'Inscrever\((\d+)', a['onclick'])
        return f'{self.URL}?prova={m.group(1)}' if m else self.URL

    def trigger(self):
        fp, get_soup = self.get_html(self.URL, suffix='get.html')

        post_fp = self._repo / f'{date.today().isoformat()}-post.html'
        if not self._is_file_fresh(post_fp):
            ghash = get_soup.find('input', id='ghash')['value']
            resp = cf_requests.post(self.URL, data={
                'ic': '', 'ghash': ghash,
                'cpf': self._CPF, 'datanascimento': self._NASC, 'sexo': 'Masculino',
            })
            post_fp.write_bytes(resp.content)

        _, post_soup = self.get_html(self.URL, suffix='post.html')
        return [self.parse(post_soup, fp)]


class DrMtbRace(_DesafioRuralBase):
    URL = 'https://inscricoes.desafiorural.com.br/'
    REPO = Path('inscricoes.desafiorural.com.br')

    def title(self, soup) -> str:
        label = soup.find('label', id='labelcpf')
        if label:
            strong = label.find('strong')
            if strong:
                return strong.get_text(strip=True)
        return self._well(soup).find_all('h3')[2].get_text(strip=True)


class DrExtremo(_DesafioRuralBase):
    URL = 'https://extremo.desafiorural.com.br/'
    REPO = Path('extremo.desafiorural.com.br')


class TimerRacing():
    URL = 'https://www.timerracing.com.br/resultados-eventos'
    META = {
        'Category': 'Empresa de Cronometragem',
        'DDD': '71',
    }

class ProTiming():
    URL = 'https://www.protiming.com.br/calendario-eventos'
    META = {
        'Category': 'Empresa de Cronometragem',
        'DDD': '47',
    }

class ChipVale():
    URL = 'https://www.chipvale.com.br/resultados'
    META = {
        'Category': 'Empresa de Cronometragem',
        'DDD': '12',
    }


class SportChip():
    URL = 'https://sportchip.net/'
    META = {
        'Category': 'Empresa de Cronometragem',
        # Portugal
    }


class ChipTiming():
    URL = 'https://www.chiptiming.com.br/calendario'
    META = {
        'Category': 'Empresa de Cronometragem',
        'Tags': ['Corrida a pé',],
        'DDD': '11',
    }


class FPCiclismo(Crawler, Extractor):
    URL = 'https://fpciclismo.org.br/'
    REPO = Path('fpciclismo.org.br')

    def title(self, soup) -> str:
        return soup[1]

    def date(self, soup) -> str:
        return soup[0]

    def local(self, soup) -> str:
        return soup[3]

    def url(self, soup) -> str:
        return soup[4]

    @staticmethod
    def parse_html(soup):
        iframe = soup.find('iframe')
        iframe_src = iframe.get('data-lazy-src')
        pdf_url = iframe_src.split('file=')[1]
        parsed_url = urlparse(pdf_url)
        suffix = Path(parsed_url.path).name
        return pdf_url, suffix

    def get_latest_pdf(self, category):
        endpoint = urljoin(self.URL, f'index.php/{category}')
        fp, soup = self.get_html(endpoint, suffix=f'{category}.html')
        pdf_url, suffix = self.parse_html(soup)
        return self.get_pdf(pdf_url, suffix=suffix)

    @staticmethod
    def sanitize_calendar(raw_data, format_date = False):

        m = ["JANEIRO", "FEVEREIRO", "MARÇO", "ABRIL", "MAIO", "JUNHO","JULHO", "AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO"]
        months = dict(zip(m, range(1, 13)))
        # Typo
        months['NOVEMEBRO'] = 11

        sanitize_list = []
        # Skip headers
        for row in raw_data:
            if not row[0]:
                continue
            head = row[0].strip().upper()

            # HEADER
            if head in months:
                month = months.get(head, head)

            # EVENT
            if all(row[-3:]) and re.search(r'\d', row[0]):
                row[0] = f'{head}/{month}/2025' if format_date else row[0]
                sanitize_list.append(row)

        return sanitize_list

    def trigger(self):

        events_acc = []
        for category in {'calendario-mtb', 'calendario-estrada'}:
            fn, raw_data = self.get_latest_pdf(category)
            format_date = category in {'calendario-estrada'}
            sanitize_list = FPCiclismo.sanitize_calendar(raw_data, format_date)
            for content in sanitize_list:
                event = self.parse(content, fn)
                events_acc.append(event)

        return events_acc


class Fmc(Crawler, Extractor):
    URL = 'https://fmc.org.br/calendario/'
    REPO = Path('fmc.org.br')
    META = {
        'Category': 'Federação',
        'Tags': ['MTB', 'Estrada', 'Downhill', 'BMX', 'MG'],
        'DDD': '31',
    }

    @staticmethod
    def _slug(text):
        nfkd = unicodedata.normalize('NFKD', text)
        ascii_str = nfkd.encode('ascii', 'ignore').decode()
        return re.sub(r'[^a-z0-9]+', '-', ascii_str.lower()).strip('-')

    @staticmethod
    def _first_iso_date(text):
        dates = re.findall(r'\d{2}/\d{2}/\d{4}', text)
        if not dates:
            return ''
        return datetime.strptime(dates[0], '%d/%m/%Y').strftime('%Y-%m-%d')

    def _cells(self, soup):
        return [td.get_text(strip=True) for td in soup.find_all('td')]

    def title(self, soup) -> str:
        return self._cells(soup)[4]

    def date(self, soup) -> str:
        return self._cells(soup)[0]

    def local(self, soup) -> str:
        return self._cells(soup)[5].replace('/', ' - ')

    def sport(self, soup) -> str:
        return self._cells(soup)[1]

    def url(self, soup) -> str:
        cells = self._cells(soup)
        iso = self._first_iso_date(cells[0])
        slug = self._slug(cells[4])
        return f'{self.URL}#{iso}-{slug}'

    def trigger(self):
        fp, soup = self.get_html(self.URL, suffix='calendario.html')
        table = soup.find('table')
        rows = table.find_all('tr')
        events_acc = []
        for row in rows:
            cells = [td.get_text(strip=True) for td in row.find_all('td')]
            if len(cells) != 9 or not cells[4] or cells[4] == 'EVENTO':
                continue
            events_acc.append(self.parse(row, fp))
        return events_acc


class FpcParana(Crawler, Extractor):
    URL = 'https://fpcparana.com.br/eventos/'
    REPO = Path('fpcparana.com.br')
    META = {
        'Category': 'Federação',
        'Tags': ['MTB', 'Estrada', 'Downhill', 'BMX', 'PR'],
        'DDD': '41',
    }

    def title(self, soup) -> str:
        return soup.find('h3').find('a').get_text(strip=True)

    def date(self, soup) -> str:
        for span in soup.find_all('span'):
            if span.find('i', class_=lambda c: c and 'fa-calendar' in c):
                return span.get_text(strip=True)
        return ''

    def local(self, soup) -> str:
        return ''

    def url(self, soup) -> str:
        return soup.find('h3').find('a')['href']

    def sport(self, soup) -> str:
        cats = soup.find('div', class_='wd-product-cats')
        return ', '.join(a.get_text(strip=True) for a in cats.find_all('a'))

    def trigger(self):
        fp, soup = self.get_html(f'{self.URL}?per_page=100', suffix='eventos.html')
        cards = soup.find_all('div', class_='product-element-bottom')
        return [self.parse(card, fp) for card in cards]
