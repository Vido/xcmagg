from pprint import pprint

from engine import Parser
from cronos import *
from aggregators import *

from itertools import chain
flatten = chain.from_iterable

def main():
    print("Hello from xcmagg!")

    crawlers = [
        #TIOnline(),
        #Peloto(),
        #ActiveSports(),
        #CorridaPronta(),
        #TourDoPeixe(),
        #TicketBr(),
        FPCiclismo(),
    ]

    for crawler in crawlers:
        events = crawler.trigger()
        print(crawler, "Done!")
        #pprint(events)
        crawler.store(events)

    parser = Parser()
    bronze_jsonl = parser.aggregate(crawlers)
    agg = [parser.process_all(x) for x in bronze_jsonl]

    from IPython import embed; embed()

if __name__ == "__main__":
    main()
