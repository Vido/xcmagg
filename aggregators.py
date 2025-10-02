import re
from pathlib import Path
from datetime import datetime

from bs4 import BeautifulSoup

from engine import Event, Crawler, Parser


class FederacaoPaulista():
    URL = 'https://fpciclismo.org.br/index.php/calendario-estrada/'
    REPO = Path('fpciclismo.org.br')
    TIME_FORMAT = '%d/%m/%Y - %H:%M'
    META = {
        'Category': 'Federação',
        'DDD': '11',
    }

class FederacaoMineira():
    URL = 'https://fmc.org.br/calendario/'
    REPO = Path('fmc.org.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Federação',
        'DDD': '31',
    }

class Sprinta():
    URL = 'https://www.ticketsports.com.br/Calendario/Todos-os-organizadores/Ciclismo,Mountain-bike/SP/'
    REPO = Path('sprinta.com.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Agregador',
        'DDD': '51',
    }

class TicketSports():
    URL = 'https://www.ticketsports.com.br/Calendario/Todos-os-organizadores/Ciclismo,Mountain-bike/SP/'
    REPO = Path('peloto.com.br')
    TIME_FORMAT = '%d/%m/%Y'
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
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Organizador',
        'DDD': '11',
    }


class TourDoPeixe(Crawler, Parser):
    URL = 'https://tourdopeixe.com.br'
    REPO = Path('tourdopeixe.com.br')
    TIME_FORMAT = '%d/%m/%Y'
    META = {
        'Category': 'Organizador',
        'DDD': '11',
    }

    def title(self, soup) -> str:
        return soup.find_all('td')[1].text.strip()
    
    def date(self, soup) -> datetime:
        datetime_str = soup.find_all('td')[0].text.strip()
        return datetime.strptime(datetime_str, self.TIME_FORMAT)

    def local(self, soup) -> str:
        return soup.find_all('td')[3].text.strip()
    
    def url(self, soup) -> str:
        if not soup.find('a'):
            return '#'
        return soup.find('a').get('href')

    def trigger(self):
        endpoint = self.URL + '/calendario/'
        html = self.get_html(endpoint, suffix='calendario')
        soup = BeautifulSoup(html, "lxml")
        tr = soup.find('table').find_all('tr')

        events_acc = []
        for i, t in enumerate(tr):
            if not i:
                continue # header
            try:
                events_acc.append(self.parse(t))
            except ValueError as ve:
                continue

        return events_acc
