from pprint import pprint

from cronos import *
from aggregators import *

def main():
    print("Hello from xcmagg!")

    crawlers = [
        TIOnline(),
        Peloto(),
        ActiveSports(),
        CorridaPronta(),
        TourDoPeixe(),
        TicketBr(),
    ]

    for crawler in crawlers:
        events = crawler.trigger()
        print("Done!")
        pprint(events)

    from IPython import embed; embed()

if __name__ == "__main__":
    main()
