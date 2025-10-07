import re
from pathlib import Path
from datetime import datetime

from bs4 import BeautifulSoup

from engine import Crawler, Extractor


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

class Sprinta():
    URL = 'https://www.ticketsports.com.br/Calendario/Todos-os-organizadores/Ciclismo,Mountain-bike/SP/'
    REPO = Path('sprinta.com.br')
    META = {
        'Category': 'Agregador',
        'DDD': '51',
    }

class TicketSports():
    URL = 'https://www.ticketsports.com.br/Calendario/Todos-os-organizadores/Ciclismo,Mountain-bike/SP/'
    REPO = Path('peloto.com.br')
    META = {
        'Category': 'Agregador',
        'DDD': '11',
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
            return '#'
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
