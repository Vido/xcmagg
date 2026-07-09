from django.core.management.base import BaseCommand
from nodes.models import Node, NodeKind
from catalog.models import Category, Durability, Audience

CONSUMABLE = Durability.CONSUMABLE
DURABLE = Durability.DURABLE
BIKE = Audience.BIKE
PARTS = Audience.PARTS
RIDER = Audience.RIDER

SEED_CATEGORIES = [
    # ----- Rider consumables -----
    {
        "title": "Nutrition",
        "description": "Energy gels, bars, chews, and recovery fuel for riding.",
        "durability": CONSUMABLE,
        "audience": RIDER,
    },
    {
        "title": "Hydration",
        "description": "Electrolyte and drink mixes for riding.",
        "durability": CONSUMABLE,
        "audience": RIDER,
    },
    {
        "title": "Supplements",
        "description": "Vitamins, minerals, protein, and recovery supplements for cyclists.",
        "durability": CONSUMABLE,
        "audience": RIDER,
    },
    # ----- Bicycles -----
    {
        "title": "Road Bikes",
        "description": "Road bikes for tarmac riding, racing, and endurance.",
        "durability": DURABLE,
        "audience": BIKE,
    },
    {
        "title": "Gravel Bikes",
        "description": "Gravel and adventure bikes for mixed-surface riding.",
        "durability": DURABLE,
        "audience": BIKE,
    },
    {
        "title": "Mountain Bikes",
        "description": "MTBs for trail, enduro, XC, and downhill riding.",
        "durability": DURABLE,
        "audience": BIKE,
    },
    # ----- Bike consumables -----
    {
        "title": "Chains",
        "description": "Drivetrain chains and quick links across speeds.",
        "durability": CONSUMABLE,
        "audience": PARTS,
    },
    {
        "title": "Brake Pads",
        "description": "Disc and rim brake pads for road, gravel, and MTB.",
        "durability": CONSUMABLE,
        "audience": PARTS,
    },
    {
        "title": "Tires",
        "description": "Clincher, tubeless, and tubular tires.",
        "durability": CONSUMABLE,
        "audience": PARTS,
    },
    {
        "title": "Tubes",
        "description": "Inner tubes, including TPU and butyl.",
        "durability": CONSUMABLE,
        "audience": PARTS,
    },
    {
        "title": "Tubeless & Sealant",
        "description": "Tubeless valves, sealant, plugs, and conversion kits.",
        "durability": CONSUMABLE,
        "audience": PARTS,
    },
    {
        "title": "Lubes & Cleaning",
        "description": "Chain lubricants, degreasers, cleaners, and drivetrain care.",
        "durability": CONSUMABLE,
        "audience": PARTS,
    },
    {
        "title": "Bar Tape & Grips",
        "description": "Handlebar tape and grips for road and off-road bikes.",
        "durability": CONSUMABLE,
        "audience": PARTS,
    },
    # ----- Bike durables -----
    {
        "title": "Bars & Stems",
        "description": "Handlebars, stems, and cockpit components for road, gravel, and MTB.",
        "durability": DURABLE,
        "audience": PARTS,
    },
    {
        "title": "Pumps & Inflators",
        "description": "Floor, mini, and electric pumps plus CO2 inflators.",
        "durability": DURABLE,
        "audience": PARTS,
    },
    {
        "title": "Workshop Tools",
        "description": "Stand-mounted and bench tools for home maintenance.",
        "durability": DURABLE,
        "audience": PARTS,
    },
    {
        "title": "Bottles & Cages",
        "description": "Bidons and cages for the bike.",
        "durability": DURABLE,
        "audience": PARTS,
    },
    {
        "title": "Bags & Storage",
        "description": "Saddle bags, frame bags, top tube bags, and bikepacking storage.",
        "durability": DURABLE,
        "audience": PARTS,
    },
    {
        "title": "Lights & Radar",
        "description": "Front/rear lights and rear-facing radar units.",
        "durability": DURABLE,
        "audience": PARTS,
    },
    {
        "title": "Computers & Sensors",
        "description": "GPS head units, power meters, and speed/cadence/HR sensors.",
        "durability": DURABLE,
        "audience": PARTS,
    },
    {
        "title": "Saddles",
        "description": "Road, gravel, and MTB saddles for comfort and performance.",
        "durability": DURABLE,
        "audience": PARTS,
    },
    {
        "title": "Pedals & Cleats",
        "description": "Clipless and flat pedals plus replacement cleats.",
        "durability": DURABLE,
        "audience": PARTS,
    },
    # ----- Rider durables -----
    {
        "title": "Portable Tools",
        "description": "Multitools, tire levers, and saddlebag spares for trailside fixes.",
        "durability": DURABLE,
        "audience": RIDER,
    },
    {
        "title": "Helmets",
        "description": "Road, gravel, and MTB helmets for protection.",
        "durability": DURABLE,
        "audience": RIDER,
    },
    {
        "title": "Eyewear",
        "description": "Cycling sunglasses, goggles, and visors.",
        "durability": DURABLE,
        "audience": RIDER,
    },
    {
        "title": "Apparel",
        "description": "Jerseys, bibs, and cycling kit.",
        "durability": DURABLE,
        "audience": RIDER,
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
                    "durability": data["durability"],
                    "audience": data["audience"],
                },
            )

        self.stdout.write(self.style.SUCCESS("Seeded cycling categories"))
