import os
import time
from pathlib import Path
from itertools import chain
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from datetime import date, datetime, timedelta

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Iterator, Tuple
from dataclasses import dataclass, asdict

import json
import requests
import validators
import jsonlines
import pdfplumber
from bs4 import BeautifulSoup


@dataclass
class RawEvent:
    title: str
    local: str
    date: str
    url: str
    source: str
    crawled_at: datetime
    raw_file: Path

    def __post_init__(self):
        self.validate()

    def validate(self):

        if not self.title:
            raise ValueError('Title cannot be empty')

        if not self.url:
            raise ValueError('URL cannot be empty')

        parsed = urlparse(self.url)
        if not parsed.scheme:
            self.url = "https://" + self.url
            parsed = urlparse(self.url)

        if not parsed.netloc:
            raise ValueError(f'Malformed URL: {self.url}')

        if not validators.url(self.url):
            raise ValueError(f'Invalid URL: {self.url}')


    def to_dict(self):
        d = asdict(self)
        d['crawled_at'] = self.crawled_at.isoformat()
        d['raw_file'] = str(self.raw_file)
        return d


class RawLayer:

    BASE = Path(__file__).parent / 'data' / 'bronze'

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

    def latest(self, glob='*.html'):
        return sorted(Path(self._repo).glob(glob),
                key=os.path.getctime, reverse=True)


class Crawler(ABC, RawLayer):

    # TODO:
    # set crawl_delay based on robots.txt

    @staticmethod
    def _call(method_f, endpoint, params={}, payload={}, crawl_delay=1):
        time.sleep(crawl_delay)
        kwargs = {
            'headers': {
                #'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:141.0) Gecko/20100101 Firefox/141.0',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                #'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Language': 'pt-BR,en-US;q=0.8,en;q=0.6,pt;q=0.4',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'cross-site',
                'Priority': 'u=0, i',
                #'Pragma': 'no-cache',
                #'Cache-Control': 'no-cache'
            },
        }
        kwargs.update({'params': params}) if params else None
        kwargs.update({'data': json.dumps(payload)}) if payload else None
        return method_f(endpoint, **kwargs)


    def download(self, url, suffix, method_f=requests.get, **kwargs) -> Path:

        # Cached
        latest = self.latest(glob=f'*{suffix}')
        if latest:
            last = max(latest)
            if self._is_file_fresh(last):
                print(f'Reading from: {last}')
                return last

        print(f'Requesting {url}')
        response = Crawler._call(method_f, url, **kwargs)
        response.raise_for_status()

        today = date.today().isoformat()
        fn = self._repo / f'{today}-{suffix}'
        fn.write_bytes(response.content)
        return fn

    def get_html(self,
            url: str,
            suffix: str ='home.html',
            encoding: str | None = 'utf-8') -> Tuple[Path, BeautifulSoup]:
        fn = self.download(url, suffix)
        html = fn.read_text(encoding=encoding)
        return fn, BeautifulSoup(html, "lxml")

    def get_pdf(self, url, suffix='doc.pdf') -> Tuple[Path, List]:
        fn = self.download(url, suffix)
        raw_data = []
        with pdfplumber.open(fn) as pdf:
            for page in pdf.pages:
                raw_data += page.extract_table()
        return fn, raw_data

    def get_json(self, url, suffix='eventos.json', payload=None) -> Tuple[Path, List]:
        method_f = requests.post if payload else requests.get
        fn = self.download(url, suffix, method_f=method_f, params=payload)
        text = fn.read_text(encoding='utf-8')
        data = json.loads(text)
        return fn, data

    @abstractmethod
    def trigger(self) -> List[RawEvent]:
        raise NotImplementedError


class BronzeLayer:

    BASE = Path(__file__).parent / 'data' / 'bronze'

    @classmethod
    def store_jsonl(klass,
            event_list: List[RawEvent], repo: Path | str = '') -> Path:
        today = date.today().isoformat()
        fn = klass.BASE / repo / f'{today}.jsonl'
        with jsonlines.open(fn, mode='w') as writer:
            writer.write_all([e.to_dict() for e in event_list])
        return fn

    @classmethod
    def store_db(klass, events_jsonl: Path):
        from db import Persistence
        p = Persistence()
        results = p.store_raw_events(events_jsonl)
        p._vacuum() # Optional
        return results

    @classmethod
    def load_new_events(klass):
        from db import Persistence
        p = Persistence()
        try:
            results = p.load_new_events()
        except: # TODO
            results = p.load_all_events()

        return results

    @staticmethod
    def collect_all(bronze_events: List[Crawler]) -> List[RawEvent]:
        """ Historical """
        return sum([repo.lastest(glob='../*.jsonl') for repo in bronze_events], [])

    @staticmethod
    def collect(bronze_events: List[Crawler]) -> List[RawEvent]:
        """ Only the Last """
        return [max(repo.latest(glob='../*.jsonl')) for repo in bronze_events]


class Extractor(ABC):

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
