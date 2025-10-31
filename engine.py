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


    def download(self, url, suffix) -> Path:

        # Cached
        latest = self.latest(glob=f'*{suffix}')
        if latest:
            last = max(latest)
            if self._is_file_fresh(last):
                print(f'Reading from: {last}')
                return last

        print(f'Requesting {url}')
        response = Crawler._call(requests.get, url)
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

    def get_json(self, url, suffix='eventos.json') -> Tuple[Path, List]:
        fn = self.download(url, suffix)
        text = fn.read_text(encoding='utf-8')
        data = json.loads(text)
        return fn, data

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
        fn = self._bronze / f'{today}.jsonl'
        with jsonlines.open(fn, mode='w') as writer:
            writer.write_all([e.to_dict() for e in event_list])
        return fn


class Extractor(ABC, BronzeLayer):

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


@dataclass
class DateRange:
    date_raw: str
    multi_day: Optional[bool] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None

    def __post_init__(self):
        if not self.date_raw:
            raise ValueError("date_raw cannot be empty")

    def to_dict(self):
        d = asdict(self)
        d['start_date'] = self.start_date.strftime("%d-%m-%Y") if self.start_date else ''
        d['end_date'] = self.end_date.strftime("%d-%m-%Y") if self.end_date else ''
        return d

@dataclass
class Location:
    location_raw: str
    address: Optional[str] = None
    city: Optional[str] = None
    uf: Optional[str] = None

    def __post_init__(self):
        if not self.location_raw:
            raise ValueError("location_raw cannot be empty")

    def to_dict(self):
        return asdict(self)

@dataclass
class SchemaEvent:
    """Cleaned, validated, standardized event"""
    title: str
    url: str
    source: str
    date_range: DateRange
    location: Location
    processed_at: datetime
    crawled_at: datetime
    bronze_file: Optional[Path] = None

    def to_dict(self):
        d = asdict(self)
        d['processed_at'] = self.processed_at.isoformat()
        d['bronze_file'] = str(self.bronze_file)
        d['date_range'] = self.date_range.to_dict()
        d['location'] = asdict(self.location)

        return d

class SilverLayer(ABC):

    BASE = Path(__file__).parent / 'data'

    def __init__(self):
        self.unique_events = set()
        self._silver = self.BASE / 'silver'
        self._silver.mkdir(parents=True, exist_ok=True)
        super().__init__()

    def collect_all(self, bronze_events: List[Crawler]) -> List[RawEvent]:
        """ Historical """
        return sum([repo.lastest(glob='../*.jsonl') for repo in bronze_events], [])

    def collect(self, bronze_events: List[Crawler]) -> List[RawEvent]:
        """ Only the Last """
        return [max(repo.latest(glob='../*.jsonl')) for repo in bronze_events]

    def aggregate_jsonl(self, event_list: List[SchemaEvent]) -> Path:
        # Normalize?

        today = date.today().isoformat()
        fn = self._silver / f'{today}.jsonl'
        with jsonlines.open(fn, mode='w') as fp:
            # TODO: Custom decoder
            objs = [e.to_dict() for e in chain(*event_list)]
            objs.sort(key=lambda x: x['crawled_at'], reverse=True)
            fp.write_all(objs)
        return fp

    def store_sql(self, event_list: List[SchemaEvent]):
        # duckdb?
        raise NotImplementedError


class DuplicatedEvent(ValueError):
    """A Duplciated Event was found"""


class Parser(SilverLayer):

    def __post_init__(self):
        if not self.title:
            raise ValueError("Title cannot be empty")

        if not self.url:
            raise ValueError("URL cannot be empty")

    def title(self, raw_event) -> str:
        return raw_event.title

    def url(self, raw_event) -> str:
        parsed = urlparse(raw_event.url)
        query = parse_qs(parsed.query)
        query["utm_source"] = ['xcmagg']
        new_query = urlencode(query, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    def source(self, raw_event) -> str:
        return raw_event.source

    def date_range(self, raw_event) -> DateRange:
        from agents import normalize_daterange
        llm_parsed = normalize_daterange(f'{raw_event.date} {raw_event.title}')
        llm_parsed['date_raw'] = raw_event.date
        print(llm_parsed)
        return DateRange(**llm_parsed)

    def location(self, raw_event) -> Location:
        from agents import normalize_location
        llm_parsed = normalize_location(f'{raw_event.local} {raw_event.title}')
        llm_parsed['location_raw'] = raw_event.local
        print(llm_parsed)
        return Location(**llm_parsed)

    def processed_at(self) -> datetime:
        return datetime.now()

    def bronze_file(self, fp: Path) -> Path:
        return fp.resolve()

    def dedup_strategy(self, event: RawEvent) -> bool:
        """ Basic deduplication: Checks for duplicated URLS """
        superkey = (event.title, event.url)
        if superkey in self.unique_events:
            return True
        self.unique_events.add(superkey)
        return False

    def process(self, raw_event: RawEvent, source: Path) -> SchemaEvent:
        event = None
        try:
             event = SchemaEvent(
                title=self.title(raw_event),
                location=self.location(raw_event),
                date_range=self.date_range(raw_event),
                url=self.url(raw_event),
                source=self.source(raw_event),
                crawled_at=raw_event.crawled_at,
                processed_at=datetime.now(),
                bronze_file=self.bronze_file(source)
             )
        except Exception as e:
            raise

        return event

    def process_all(self, jsonlfile: Path) -> Iterator[SchemaEvent]:
        with jsonlines.open(jsonlfile) as reader:
            for line, obj in enumerate(reader):
                try:
                    raw_event = RawEvent(**obj)
                    raw_event.validate()
                    if self.dedup_strategy(raw_event):
                        raise DuplicatedEvent(f'Duplicated: {raw_event.title}: {raw_event.url}')
                    event = self.process(raw_event, jsonlfile)
                except DuplicatedEvent as D:
                    print(D)
                    print(f'{jsonlfile.resolve()} - Line: {line}')
                    continue
                except Exception as E:
                    print(E)
                    print(f'{jsonlfile.resolve()} - Line: {line}')
                    continue

                yield event
