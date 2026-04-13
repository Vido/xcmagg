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
    # TourDoPeixe(), # Morreu. Usava TicketSport
    TicketBr(),
    # FPCiclismo(), # 503
    SeuEsporteApp(),
    TicketSportsAPI2(),
    InscricoesBike(),
    Nuflow(),
    Atletis(),
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

from agents import SPORTS
RELEVANT_SPORTS = set(SPORTS.values()) | {''}  # '' = crawlers that don't set sport pass through unchanged

def load_v2():
    parser = Parser()

    agg = []
    for obj in BronzeLayer.load_new_events():
        if obj['sport'] not in RELEVANT_SPORTS:
            continue
        agg.append(parser.process(obj))

    for obj in BronzeLayer.load_low_quality_events():
        agg.append(parser.process(obj))

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
