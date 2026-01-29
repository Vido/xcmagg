import re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, urljoin

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
            if text:
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
        # First link inside <tr class="efeito">
        tr = soup.find('tr', class_='efeito')
        a = tr.find('a')
        return f"{self.URL}{a['href']}"

    def trigger(self):

        def parse(buffer, fp):
            from bs4 import BeautifulSoup
            return self.parse(BeautifulSoup(buffer, 'lxml'), fp)

        fp, soup = self.get_html(self.URL, suffix='home.html')
        events_acc, buffer = [], ''

        rows = soup.find('table').find_all('tr', recursive=False)
        for tr in rows:
            # reset buffer / new event
            if 'efeito' in tr.get('class', []) and buffer:
                event = parse(buffer, fp)
                events_acc.append(event)
                buffer = ''
            buffer += str(tr)

        # Leftover in the buffer
        if buffer:
            event = parse(buffer, fp)
            events_acc.append(event)

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
        return div.find('h4').text.strip()
    
    def date(self, soup) -> str:
        div = soup.find('div', class_='row red darken-4 white-text center')
        return div.find_all('h5')[1].text.strip()

    def local(self, soup) -> str:
        local = soup.find('div', class_='col s12 m8 l8 white-text')
        return local.find('div', class_='card').find('h5').text.strip()
   
    def url(self, soup) -> str:
        #class_ = 'btn-large waves-effect waves-light blue white-text pulse col s12' 
        href = soup.find('a', class_='btn-large').get('href')
        return urljoin(self.URL, href)

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
        return soup.find('h5').text.strip()
    
    def date(self, soup) -> str:
        try:
            return soup.find('h4').text.strip()
        except AttributeError:
            return soup.find('h5').text.strip()
        
    def local(self, soup) -> str:
        local = soup.find('div', class_='cidade').text.strip()
        return local.replace('Cidade: ', '')
    
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
