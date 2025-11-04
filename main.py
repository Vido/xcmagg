from pprint import pprint

from cronos import *
from aggregators import *
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
    TicketSports(),
    InscricoesBike(),
    Nuflow(),
]

def extract():
    for crawler in crawlers:
        events = crawler.trigger()
        print(crawler, "Done!")
        #pprint(events)
        crawler.store(events)

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
