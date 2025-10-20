import re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

from engine import Crawler, Extractor


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
        return self.URL + 'inscricao/' + soup.find('a').get('href')

    def trigger(self):
        endpoint = self.URL + 'eventos.php'
        fp, soup = self.get_html(endpoint, suffix='eventos')
        div = soup.find_all('div', 's-12 m-6 l-3')

        href_list = []
        for d in div:
            href = d.find('a').get('href')
            if href != 'http://www.fbresportes.com':
                href_list.append(href)

        events_acc = []
        for href in href_list:
            url = f"{self.URL}/{href}"
            fn = re.sub(r'(?u)[^-\w.]', '_', href)
            fp, soup2 = self.get_html(url, suffix=fn)
            events_acc.append(self.parse(soup2, fp))
        return events_acc


class ActiveSports(Crawler, Extractor):
    URL = 'https://www.activesports.com.br'
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
        return self.URL + soup[0].get('href')

    def trigger(self):
        endpoint = self.URL + '/proximos-eventos'
        fp, soup = self.get_html(self.URL, suffix='proximos-eventos')
        div = soup.find_all('div', class_='content-course')

        events_acc = []
        for d in div:
            events_acc.append(self.parse(d.find_all('a'), fp))

        return events_acc


class GpsControlCrono():
    URL = 'https://www.gpscontrolcrono.com.br'
    REPO = Path('gpscontrolcrono.com.br')
    TIME_FORMAT = '%d/%m/%Y'

    def title(self, soup) -> str:
        return soup.find('div', class_='title').text.strip()
    
    def date(self, soup) -> str:
        return d.find('div', class_='hours').text.strip()

    def local(self, soup) -> str:
        return soup.find('div', class_='local').text.strip()
    
    def url(self, soup) -> str:
        return soup.find('a').get('href')

    def trigger(self):
        html = self.get_html(self.URL, suffix='home.html')
        fp, soup = BeautifulSoup(html.read_text(), "lxml")

        events_acc = []
        div = soup.find_all('div', 'slider__footer')
        for d in div:
            datetime_str = d.find('div', class_='hours').text.strip()
            events_acc.append(self.parse(d), fp)


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
    URL = 'https://peloto.com.br'
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
        return soup.find('a', class_='btn-large').get('href')

    def trigger(self):

        fp, soup = self.get_html(self.URL, suffix='home.html')
        div = soup.find_all('div', 'prox-eventos')

        href_list = []
        for d in div:
            href = d.find('a').get('href')
            href_list.append(href)

        events_acc = []
        for href in href_list:
            url = f"{self.URL}/{href}"
            fn = re.sub(r'(?u)[^-\w.]', '_', href)
            fp, soup2 = self.get_html(url, suffix=fn)
            events_acc.append(self.parse(soup2, fp))

        return events_acc


class ProximaProva():
    URL = 'https://proximaprova.com.br/eventos/'


class Nuflow():
    URL = 'https://nuflowpass.com.br/events'
    REPO = Path('nuflowpass.com.br')
    TIME_FORMAT = '%d/%m/%Y %H:%M'
    META = {
        'Category': 'Empresa de Cronometragem',
        'DDD': '31',
    }


class TicketBr(Crawler, Extractor):
    URL = 'https://www.ticketbr.com.br'
    REPO = Path('ticketbr.com.br')
    TIME_FORMAT = '%d/%m/%Y %H:%M'
    META = {
        'Category': 'Empresa de Ingressos',
        'DDD': '16',
    }
    def title(self, soup) -> str:
        return soup.find('h5').text.strip()
    
    def date(self, soup) -> str:
        return soup.find('h4').text.strip()
        
    def local(self, soup) -> str:
        local = soup.find('div', class_='cidade').text.strip()
        return local.replace('Cidade: ', '')
    
    def url(self, soup) -> str:
        pk = soup.find('div', class_='inscricao').find('a')
        if not pk:
            return '#'
        pk = ''.join(filter(str.isdigit, pk.get('onclick')))
        return f'https://www.ticketbr.com.br/evento/undefined/{pk}'

    def trigger(self):
        from bs4 import BeautifulSoup
        endpoint = self.URL + '/calendario.ecm'
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

    def trigger(self):
        endpoint = self.URL + 'index.php/calendario-mtb/'
        fp, soup = self.get_html(endpoint, suffix='calendario-mtb')
        pdf_url, suffix = FPCiclismo.parse_html(soup)
        fn, raw_data = self.get_pdf(pdf_url, suffix=suffix)

        filtered_list = []
        for row in raw_data:
            if all(row[-3:]) and re.match(r'^\d', row[0].strip()):
                filtered_list.append(row)

        events_acc = []
        for content in filtered_list:
            event = self.parse(content, fp)
            events_acc.append(event)

        return events_acc


