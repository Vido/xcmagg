from django.core.management.base import BaseCommand
from nodes.models import Node, NodeKind
from catalog.models import Category

SEED_CATEGORIES = [
    {
        "title": "Pumps & Inflation",
        "description": "Floor pumps, mini pumps, CO2 inflators, and pressure gauges.",
    },
    {
        "title": "Tools & Maintenance",
        "description": "Multitools, chain tools, torque wrenches, and workshop tools.",
    },
    {
        "title": "Lube & Cleaning",
        "description": "Chain lubricants, degreasers, cleaners, and drivetrain care.",
    },
    {
        "title": "Bottles & Hydration",
        "description": "Bidons, cages, and hydration systems for riding.",
    },
    {
        "title": "Bags & Storage",
        "description": "Saddle bags, frame bags, top tube bags, and bikepacking storage.",
    },
    {
        "title": "Lights",
        "description": "Front and rear lights for visibility and night riding.",
    },
    {
        "title": "Computers & Sensors",
        "description": "GPS head units, power meters, and speed/cadence/HR sensors.",
    },
    {
        "title": "Tires & Tubes",
        "description": "Clincher, tubeless, and tubular tires plus inner tubes.",
    },
    {
        "title": "Tubeless & Sealant",
        "description": "Tubeless valves, sealant, plugs, and conversion kits.",
    },
    {
        "title": "Saddles",
        "description": "Road, gravel, and MTB saddles for comfort and performance.",
    },
    {
        "title": "Bar Tape & Grips",
        "description": "Handlebar tape and grips for road and off-road bikes.",
    },
    {
        "title": "Pedals & Cleats",
        "description": "Clipless and flat pedals plus replacement cleats.",
    },
    {
        "title": "Helmets",
        "description": "Road, gravel, and MTB helmets for protection.",
    },
]

class Command(BaseCommand):
    help = "Seed canonical cycling categories"

    def handle(self, *args, **options):
        for data in SEED_CATEGORIES:
            node, _ = Node.objects.update_or_create(
                title=data["title"],
                kind=NodeKind.CATEGORY,
                defaults={},
            )

            Category.objects.update_or_create(
                node=node,
                defaults={
                    "description": data["description"],
                },
            )

        self.stdout.write(self.style.SUCCESS("Seeded cycling categories"))
