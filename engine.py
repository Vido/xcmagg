import os
import time
from pathlib import Path
from datetime import date, datetime, timedelta

from abc import ABC, abstractmethod
from typing import Dict, Any, List
from dataclasses import dataclass

import requests


@dataclass
class Event:
    title: str
    local: str
    date: datetime
    url: str
    source: str
    
    def __post_init__(self):
        if not self.title:
            raise ValueError("Title cannot be empty")

        if not self.url:
            raise ValueError("URL cannot be empty")


class Crawler(ABC):

    BASE = Path(__file__).parent / 'data'

    def __init__(self):
        if hasattr(self, 'REPO'):
            self._repo = self.BASE / self.REPO / 'raw'
            self._repo.mkdir(parents=True, exist_ok=True)
        else:
            raise AttributeError("REPO must be defined in concrete class") 

    @staticmethod
    def _call(method_f, endpoint, params={}, payload={}):
        kwargs = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:141.0) Gecko/20100101 Firefox/141.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'cross-site',
                'Priority': 'u=0, i',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache'
            },
        }
        kwargs.update({'params': params}) if params else None
        kwargs.update({'data': json.dumps(payload)}) if payload else None
        return method_f(endpoint, **kwargs)

    @staticmethod
    def _is_file_fresh(filepath, max_age_hours=23):
        if not filepath.exists():
            return False
        age = datetime.now() - datetime.fromtimestamp(filepath.stat().st_mtime)
        return age < timedelta(hours=max_age_hours)

    @staticmethod
    def download(url, delay=1):
        print(f'Requesting {url}')
        time.sleep(delay)
        response = Crawler._call(requests.get, url)
        response.raise_for_status()
        return response.text

    def lastest(self, glob='*.html'):
        return sorted(Path(self._repo).glob(glob), key=os.path.getctime)

    def get_html(self, url, suffix='home.html'):

        # Cached
        lastest = self.lastest(glob=f'*{suffix}')
        if lastest:
            last = max(lastest)
            if self._is_file_fresh(last):
                print(f'Reading from: {last}')
                return last.read_text()

        # Fresh
        html = Crawler.download(url)
        today = date.today().isoformat()
        filepath = self._repo / f'{today}-{suffix}'
        filepath.write_text(html)
        return html

    @abstractmethod
    def trigger(self) -> Event:
        raise NotImplementedError


class Parser(ABC):
    @abstractmethod
    def title(self, soup) -> str:
        raise NotImplementedError
    
    @abstractmethod
    def date(self, soup) -> datetime:
        raise NotImplementedError

    @abstractmethod
    def local(self, soup) -> str:
        raise NotImplementedError
    
    @abstractmethod
    def url(self, soup) -> str:
        raise NotImplementedError

    def source(self) -> str:
        return self.URL
 
    def parse(self, soup) -> Event:
        try:
            return Event(
                title=self.title(soup),
                local=self.local(soup),
                date=self.date(soup),
                url=self.url(soup),
                source=self.source(),
            )
        except Exception as e:
            print(f"Error parsing race from {self.source()}: {e}")
            # Log the problematic HTML for debugging
            print(f"Problematic HTML: {soup}")
            raise
