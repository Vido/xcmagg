import os
import time
from pathlib import Path
from itertools import chain
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from datetime import date, datetime, timedelta

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Iterator, Tuple
from dataclasses import dataclass, asdict

import jsonlines

from bronze import Crawler, RawEvent


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

    def process(self, event_obj: Dict, source: Path) -> SchemaEvent:
        event = None
        raw_event = RawEvent(**event_obj)
        raw_event.validate()
        if self.dedup_strategy(raw_event):
            raise DuplicatedEvent(f'Duplicated: {raw_event.title}: {raw_event.url}')

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

        return event

    def process_all(self, jsonlfile: Path) -> Iterator[SchemaEvent]:
        with jsonlines.open(jsonlfile) as reader:
            for line, obj in enumerate(reader):
                try:
                    event = self.process(obj, jsonlfile)
                except DuplicatedEvent as D:
                    print(D)
                    print(f'{jsonlfile.resolve()} - Line: {line}')
                    continue
                except Exception as E:
                    print(E)
                    print(f'{jsonlfile.resolve()} - Line: {line}')
                    continue

                yield event