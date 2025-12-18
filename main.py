from pprint import pprint

from cronos import *
from aggregators import *
from bronze import BronzeLayer
from silver import SilverLayer, Parser

from itertools import chain
flatten = chain.from_iterable

# TODO: Use a config file
crawlers = [
    TIOnline(),
    Peloto(),
    ActiveSports(),
    CorridaPronta(),
    GpsControlCrono(),
    # TourDoPeixe(), # Uses ticketsports as provider
    TicketBr(),
    FPCiclismo(),
    SeuEsporteApp(),
    TicketSportsAPI(),
    InscricoesBike(),
    Nuflow(),
]

def extract():
    all_events = []
    for crawler in crawlers:
        events = crawler.trigger()
        BronzeLayer.store_jsonl(events, crawler.REPO)
        all_events += events
        print(crawler, "Done!")

    jsonlfile = BronzeLayer.store_jsonl(all_events)
    BronzeLayer.store_db(jsonlfile)

def load():
    """ File-based alternative:
        - Problems: Duplications
        - Problems: Reprocessing
    """
    parser = Parser()
    bronze_jsonl = parser.collect(crawlers)
    agg = [parser.process_all(x) for x in bronze_jsonl]
    # Upgrade to Silver
    parser.aggregate_jsonl(agg)

def load_v2():
    parser = Parser()

    # Deduplication / Avoid reprocessing
    agg = []
    raw_events = BronzeLayer.load_new_events()
    for obj in raw_events:
        schema_event = parser.process(obj)
        agg.append(schema_event)

    jsonlfile = SilverLayer().store_jsonl(agg)
    if agg:
        SilverLayer.store_db(jsonlfile)

def publish():
    from gold import GoldLayer
    agg = []
    schema_events = GoldLayer.publish()
    events = schema_events.fetchall()

if __name__ == "__main__":
    print("Hello from xcmagg!") 
    extract()
    load_v2()
    publish()
