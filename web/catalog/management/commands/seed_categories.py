from django.core.management.base import BaseCommand
from nodes.models import Node, NodeKind
from catalog.models import Category

SEED_CATEGORIES = [
    {
        "slug": "knives",
        "title": "Knives",
        "description": "Folding knives, fixed blades, and cutting tools for everyday carry.",
    },
    {
        "slug": "watches",
        "title": "Watches",
        "description": "Mechanical, quartz, digital, and tool watches for daily wear.",
    },
    {
        "slug": "flashlights",
        "title": "Flashlights",
        "description": "Compact and high-output lights for EDC and emergency use.",
    },
    {
        "slug": "multitools",
        "title": "Multitools",
        "description": "Multi-purpose tools combining blades, pliers, and drivers.",
    },
    {
        "slug": "wallets",
        "title": "Wallets",
        "description": "Minimalist, tactical, and traditional wallets.",
    },
    {
        "slug": "lighters",
        "title": "Lighters",
        "description": "Reusable and disposable lighters for utility and preparedness.",
    },
    {
        "slug": "key-solutions",
        "title": "Key Solutions",
        "description": "Key organizers, carabiners, and key management systems.",
    },
    {
        "slug": "bags-hucksacks",
        "title": "Bags & Hucksacks",
        "description": "Backpacks, slings, and bags designed for daily carry.",
    },
    {
        "slug": "carry-systems",
        "title": "Carry Systems",
        "description": "Pouches, organizers, and modular carry solutions.",
    },
    {
        "slug": "pry-bars",
        "title": "Pry Bars",
        "description": "Compact pry tools for leverage and utility tasks.",
    },
    {
        "slug": "pens",
        "title": "Pens",
        "description": "Durable pens designed for everyday and tactical use.",
    },
    {
        "slug": "notebooks",
        "title": "Notebooks",
        "description": "Pocket notebooks and paper goods for daily notes.",
    },
]

class Command(BaseCommand):
    help = "Seed canonical EDC categories"

    def handle(self, *args, **options):
        for data in SEED_CATEGORIES:
            node, _ = Node.objects.update_or_create(
                slug=data["slug"],
                defaults={
                    "kind": NodeKind.CATEGORY,
                    "title": data["title"],
                },
            )

            Category.objects.update_or_create(
                node=node,
                defaults={
                    "description": data["description"],
                },
            )

        self.stdout.write(self.style.SUCCESS("Seeded EDC categories"))
