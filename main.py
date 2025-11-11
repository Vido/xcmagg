from pprint import pprint

from cronos import *
from aggregators import *
from bronze import BronzeLayer
from silver import Parser

from itertools import chain
flatten = chain.from_iterable

# TODO: Use a config file
crawlers = [
    TIOnline(),
    Peloto(),
    ActiveSports(),
    CorridaPronta(),
    TourDoPeixe(),
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
    parser = Parser()
    bronze_jsonl = parser.collect(crawlers)
    agg = [parser.process_all(x) for x in bronze_jsonl]

    # Upgrade to Silver
    parser.aggregate_jsonl(agg)
    #from IPython import embed; embed()

if __name__ == "__main__":
    print("Hello from xcmagg!") 
    extract()
    load()
