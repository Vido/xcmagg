from pprint import pprint

from engine import Parser
from cronos import *
from aggregators import *

from itertools import chain
flatten = chain.from_iterable

def main():
    print("Hello from xcmagg!")

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

    for crawler in crawlers:
        events = crawler.trigger()
        print(crawler, "Done!")
        #pprint(events)
        crawler.store(events)

    parser = Parser()
    bronze_jsonl = parser.collect(crawlers)
    agg = [parser.process_all(x) for x in bronze_jsonl]

    # Upgrade to Silver
    parser.aggregate_jsonl(agg)
    #from IPython import embed; embed()

if __name__ == "__main__":
    main()
