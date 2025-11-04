import re
import json
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin


from bs4 import BeautifulSoup

from bronze import Crawler, Extractor


class FederacaoPaulista():
    URL = 'https://fpciclismo.org.br/index.php/calendario-estrada/'
    REPO = Path('fpciclismo.org.br')
    META = {
        'Category': 'Federação',
        'DDD': '11',
    }

class FederacaoMineira():
    URL = 'https://fmc.org.br/calendario/'
    REPO = Path('fmc.org.br')
    META = {
        'Category': 'Federação',
        'DDD': '31',
    }
class ASCABiker():
    URL = 'https://ascabiker.com.br/'
    REPO = Path('ascabiker.com.br')
    META = {
        'Category': 'Organizador',
        'DDD': '35',
    }


class Sprinta():
    URL = 'https://www.ticketsports.com.br/Calendario/Todos-os-organizadores/Ciclismo,Mountain-bike/SP/'
    REPO = Path('sprinta.com.br')
    META = {
        'Category': 'Agregador',
        'DDD': '51',
    }

class TicketSports(Crawler, Extractor):
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

class Chelso():
    URL = 'https://chelso.com.br/site/category/provas-presenciais'
    META = {
        'Category': 'Organizador',
        'DDD': '19',
    }

class CopaInterior():
    URL = 'https://www.copainterior.com.br/'
    META = {
        'Category': 'Organizador',
        'DDD': '19',
    }


class TourDaRoca():
    URL = 'https://tourdaroca.com.br/'
    REPO = Path('tourdaroca.com.br')
    META = {
        'Category': 'Organizador',
        'DDD': '11',
    }


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




