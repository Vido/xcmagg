import os
import time
from pathlib import Path
from datetime import date, datetime, timedelta

from abc import ABC, abstractmethod
from typing import Dict, Any, List
from dataclasses import dataclass, asdict

import requests
import jsonlines
from bs4 import BeautifulSoup


@dataclass
class RawEvent:
    title: str
    local: str
    date: str
    url: str
    source: str
    crawled_at: datetime
    raw_file: str
    
    def __post_init__(self):
        if not self.title:
            raise ValueError("Title cannot be empty")

        if not self.url:
            raise ValueError("URL cannot be empty")

    def to_dict(self):
        d = asdict(self)
        d['crawled_at'] = self.crawled_at.isoformat()
        d['raw_file'] = str(self.raw_file)
        return d


class RawLayer:

    def __init__(self):
        if not hasattr(self, 'REPO'):
            raise AttributeError("REPO must be defined in concrete class")

        self._repo = self.BASE / self.REPO / 'raw'
        self._repo.mkdir(parents=True, exist_ok=True)
        super().__init__()

    @staticmethod
    def _is_file_fresh(filepath, max_age_hours=23):
        if not filepath.exists():
            return False
        age = datetime.now() - datetime.fromtimestamp(filepath.stat().st_mtime)
        return age < timedelta(hours=max_age_hours)

    def lastest(self, glob='*.html'):
        return sorted(Path(self._repo).glob(glob), key=os.path.getctime)


class Crawler(ABC, RawLayer):

    BASE = Path(__file__).parent / 'data' / 'bronze'

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
    def download(url, delay=1) -> str:
        print(f'Requesting {url}')
        time.sleep(delay)
        response = Crawler._call(requests.get, url)
        response.raise_for_status()
        return response.text

    def get_html(self, url, suffix='home.html'):

        # Cached
        lastest = self.lastest(glob=f'*{suffix}')
        if lastest:
            last = max(lastest)
            if self._is_file_fresh(last):
                print(f'Reading from: {last}')
                return last, BeautifulSoup(last.read_text(), "lxml")

        # Fresh
        html = Crawler.download(url)
        today = date.today().isoformat()
        fp = self._repo / f'{today}-{suffix}'
        fp.write_text(html)

        return today, BeautifulSoup(html, "lxml")

    @abstractmethod
    def trigger(self) -> List[RawEvent]:
        raise NotImplementedError


class BronzeLayer:

    def __init__(self):
        if not hasattr(self, 'REPO'):
            raise AttributeError("REPO must be defined in concrete class")

        self._bronze = self.BASE / self.REPO
        self._bronze.mkdir(parents=True, exist_ok=True)
        super().__init__()

    def store(self, event_list: List[RawEvent]) -> Path:
        today = date.today().isoformat()
        fp = self._bronze / f'{today}.jsonl'
        with jsonlines.open(fp, mode='w') as fp:
            fp.write_all([e.to_dict() for e in event_list])
        return fp

class Parser(ABC, BronzeLayer):

    @abstractmethod
    def title(self, soup) -> str:
        raise NotImplementedError
    
    @abstractmethod
    def date(self, soup) -> str:
        raise NotImplementedError

    @abstractmethod
    def local(self, soup) -> str:
        raise NotImplementedError
    
    @abstractmethod
    def url(self, soup) -> str:
        raise NotImplementedError

    def source(self) -> str:
        return self.URL

    def crawled_at(self, fp: Path) -> datetime:
        return datetime.fromtimestamp(fp.stat().st_mtime)

    def raw_file(self, fp: Path) -> str:
        return fp.resolve()

    def parse(self, soup: BeautifulSoup, filepath: Path) -> RawEvent:
        event = None
        try:
             event = RawEvent(
                title=self.title(soup),
                local=self.local(soup),
                date=self.date(soup),
                url=self.url(soup),
                source=self.source(),
                crawled_at=self.crawled_at(filepath),
                raw_file=self.raw_file(filepath),
            )
        except Exception as e:
            print(f"Error parsing race from {self.source()}: {e}")
            # Log the problematic HTML for debugging
            print(f"Problematic HTML: {soup}")
            raise

        return event


class SilverLayer:

    def __init__(self):
        self._silver = self.BASE / 'silver'
        self._silver.mkdir(parents=True, exist_ok=True)
        super().__init__()
