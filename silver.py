import os
import json
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
        d['start_date'] = self.start_date.strftime("%Y-%m-%d") if self.start_date else None
        d['end_date'] = self.end_date.strftime("%Y-%m-%d") if self.end_date else None
        return d

@dataclass
class Location:
    location_raw: str
    address: Optional[str] = None
    city: Optional[str] = None
    uf: Optional[str] = None
    ddd: Optional[str] = None
    confidence: Optional[str] = None

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
    sport: str = ''
    #bronze_file: Optional[Path] = None

    def __post_init__(self):
        if not self.title:
            raise ValueError("Title cannot be empty")

        if not self.url:
            raise ValueError("URL cannot be empty")

    def to_dict(self):
        d = asdict(self)
        d['processed_at'] = self.processed_at.isoformat()
        #d['bronze_file'] = str(self.bronze_file)
        d['date_range'] = self.date_range.to_dict()
        d['location'] = asdict(self.location)

        return d

class SilverLayer:

    BASE = Path(__file__).parent / 'data' / 'silver'

    def __init__(self):
        self.BASE.mkdir(parents=True, exist_ok=True)

    @classmethod
    def store_jsonl(klass, event_list: List[SchemaEvent]) -> Path:
        today = date.today().isoformat()
        fn = klass.BASE / f'{today}.jsonl'
        with jsonlines.open(fn, mode='w') as fp:
            # TODO: Custom decoder
            objs = [e.to_dict() for e in event_list]
            objs.sort(key=lambda x: x['crawled_at'], reverse=True)
            fp.write_all(objs)
        return fn

    @classmethod
    def store_db(klass, events_jsonl: Path):
        from db import Persistence
        p = Persistence()
        results = p.store_schema_events(events_jsonl)
        p._vacuum() # Optional


class Parser:

    def __init__(self):
        self._location_cache = self._load_location_cache()

    def _load_location_cache(self) -> dict:
        try:
            from db import Persistence
            rows = Persistence().CONN.execute("""
                SELECT location->>'location_raw', location
                FROM schema_events
                WHERE location->>'confidence' IN ('medium', 'high')
            """).fetchall()
            return {row[0]: json.loads(row[1]) for row in rows if row[0]}
        except Exception:
            return {}

    def title(self, raw_event) -> str:
        return raw_event.title

    def url(self, raw_event) -> str:
        return raw_event.url

    def source(self, raw_event) -> str:
        return raw_event.source

    def date_range(self, raw_event) -> DateRange:
        from agents import normalize_daterange
        llm_parsed = normalize_daterange(f'{raw_event.date} {raw_event.title}')
        return DateRange(
            multi_day=llm_parsed.multi_day,
            start_date=llm_parsed.start_date,
            end_date=llm_parsed.end_date,
            date_raw=raw_event.date,
        )

    def location(self, raw_event) -> Location:
        from agents import normalize_location, search_event_location

        if raw_event.local in self._location_cache:
            return Location(**self._location_cache[raw_event.local])

        # Level 1: nano — cheap, fast
        llm_input = f'{raw_event.title} - Local {raw_event.local}'
        llm_parsed = normalize_location(llm_input)

        # Level 2: mini — smarter, still no search
        if not llm_parsed.get('city'):
            llm_parsed = normalize_location(llm_input, model="gpt-4.1-mini")

        # Level 3: search — last resort
        # if not llm_parsed.get('city'):
        #     search_result = search_event_location(raw_event.title)
        #     llm_input = f'{llm_input} Pesquisa: {search_result}'
        #     print(f'[L3 input]  {llm_input}')
        #     llm_parsed = normalize_location(llm_input, model="gpt-4.1-mini")
        #     print(f'[L3 output] {llm_parsed}')

        if not llm_parsed.get('city'):
            print(f'[no city] {llm_input}')
            print(f'[no city] {llm_parsed}')

        llm_parsed['location_raw'] = llm_input

        if llm_parsed.get('confidence') in ('medium', 'high'):
            self._location_cache[raw_event.local] = llm_parsed

        return Location(**llm_parsed)

    def sport(self, raw_event) -> str:
        if raw_event.sport:
            return raw_event.sport
        from agents import classify_sport, search_classify_sport

        ## Level 1: nano
        #result = classify_sport(raw_event.title)
        #if result.sport and result.confidence != 'low':
        #    return result.sport

        # Level 2: 5.4-mini
        result = classify_sport(f'{raw_event.title} {raw_event.local}')
        if result.sport and result.confidence != 'low':
            return result.sport

        # Level 3: search — last resort
        # print(f'[L3 sport input]  {raw_event.title} — {raw_event.url}')
        # result = search_classify_sport(raw_event.title, raw_event.url)
        # print(f'[L3 sport output] {result}')
        # return result.sport.value if result.sport else ''
        return result.sport if result.sport else ''

    def processed_at(self) -> datetime:
        return datetime.now()

    def process(self, event_obj: Dict) -> SchemaEvent:
        event = None
        raw_event = RawEvent(**event_obj)
        raw_event.validate()

        event = SchemaEvent(
            title=self.title(raw_event),
            location=self.location(raw_event),
            date_range=self.date_range(raw_event),
            url=self.url(raw_event),
            source=self.source(raw_event),
            crawled_at=raw_event.crawled_at,
            processed_at=datetime.now(),
            sport=self.sport(raw_event),
        )

        return event
