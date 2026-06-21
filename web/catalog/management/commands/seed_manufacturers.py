from django.core.management.base import BaseCommand
from nodes.models import Node, NodeKind
from catalog.models import Manufacturer


SEED_MANUFACTURERS = [
    # Fallback brand for catalog items with no real manufacturer — keeps the
    # item routable (catalog URLs are namespaced under the brand slug).
    ("Unbranded", ""),
    ("Silca", "https://silca.cc"),
    ("Muc-Off", "https://muc-off.com"),
    ("Topeak", "https://www.topeak.com"),
    ("Lezyne", "https://www.lezyne.com"),
    ("Park Tool", "https://www.parktool.com"),
    ("Garmin", "https://www.garmin.com"),
    ("Wahoo", "https://www.wahoofitness.com"),
    ("Elite", "https://www.elite-it.com"),
    ("CamelBak", "https://www.camelbak.com"),
    ("Continental", "https://www.continental-tires.com"),
    ("Pirelli", "https://velo.pirelli.com"),
    ("Shimano", "https://bike.shimano.com"),
    ("SRAM", "https://www.sram.com"),
    ("Fizik", "https://www.fizik.com"),
    ("Selle Italia", "https://www.selleitalia.com"),
    ("Lizard Skins", "https://www.lizardskins.com"),
    ("Crankbrothers", "https://www.crankbrothers.com"),
    ("Knog", "https://www.knog.com"),
    ("Bryton", "https://www.brytonsport.com"),
    ("Finish Line", "https://www.finishlineusa.com"),
    ("WD-40 Bike", "https://wd40bike.com"),
    ("Stan's NoTubes", "https://www.notubes.com"),
    ("Orange Seal", "https://www.orangeseal.com"),
    ("Tubolito", "https://tubolito.com"),
    ("Specialized", "https://www.specialized.com"),
    ("Bontrager", "https://www.trekbikes.com/bontrager"),
    ("Abus", "https://www.abus.com"),
    ("Kask", "https://www.kask.com"),
]


class Command(BaseCommand):
    help = "Seed top cycling brands"

    def handle(self, *args, **options):
        for title, website in SEED_MANUFACTURERS:
            node, _ = Node.objects.update_or_create(
                title=title,
                kind=NodeKind.MANUFACTURER,
                defaults={},
            )

            Manufacturer.objects.update_or_create(
                node=node,
                defaults={
                    "website": website,
                },
            )

        self.stdout.write(self.style.SUCCESS("Seeded cycling brands"))
