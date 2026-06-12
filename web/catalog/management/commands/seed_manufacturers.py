from django.core.management.base import BaseCommand
from nodes.models import Node, NodeKind
from catalog.models import Manufacturer


SEED_MANUFACTURERS = {
    "knives": [
        ("Benchmade", "https://www.benchmade.com"),
        ("Spyderco", "https://www.spyderco.com"),
        ("CIVIVI", "https://www.civivi.com"),
        ("Kizer", "https://www.kizerknives.com"),
        ("ESEE Knives", "https://eseeknives.com"),
    ],
    "flashlights": [
        ("Olight", "https://www.olightstore.com"),
        ("Fenix", "https://www.fenixlighting.com"),
        ("Streamlight", "https://www.streamlight.com"),
        ("SureFire", "https://www.surefire.com"),
        ("AceBeam", "https://www.acebeam.com"),
    ],
    "multitools": [
        ("Leatherman", "https://www.leatherman.com"),
        ("Victorinox", "https://www.victorinox.com"),
        ("Gerber", "https://www.gerbergear.com"),
        ("SOG", "https://sogknives.com"),
        ("Roxon", "https://roxontool.com"),
    ],
    "watches": [
        ("Casio", "https://www.casio.com"),
        ("Seiko", "https://www.seikowatches.com"),
        ("Citizen", "https://www.citizenwatch.com"),
        ("G-Shock", "https://www.gshock.com"),
        ("Garmin", "https://www.garmin.com"),
    ],
}


class Command(BaseCommand):
    help = "Seed top EDC manufacturers"

    def handle(self, *args, **options):
        for category_slug, manufacturers in SEED_MANUFACTURERS.items():
            for title, website in manufacturers:
                node, _ = Node.objects.update_or_create(
                    title=title,
                    defaults={
                        "kind": NodeKind.MANUFACTURER,
                    },
                )

                Manufacturer.objects.update_or_create(
                    node=node,
                    defaults={
                        "website": website,
                    },
                )

        self.stdout.write(self.style.SUCCESS("Seeded EDC manufacturers"))
