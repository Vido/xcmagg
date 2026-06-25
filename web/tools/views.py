from django.shortcuts import render

from nodes.models import Node, NodeKind
from catalog.selectors import CategorySelectors


def _related_items(*slugs):
    """Catalog items from the given category nodes (used by the fuel pages)."""
    items = []
    for node in Node.objects.filter(slug__in=slugs, kind=NodeKind.CATEGORY):
        items += list(CategorySelectors.catalog_items(node)[:4])
    return items


def nutrition_calculator(request):
    return render(request, "tools/nutrition_calculator.html",
                  {"related_items": _related_items("nutrition")})


def hydration_calculator(request):
    return render(request, "tools/hydration_calculator.html",
                  {"related_items": _related_items("hydration")})


def fuel_plan(request):
    return render(request, "tools/fuel_plan.html",
                  {"related_items": _related_items("nutrition", "hydration")})


def calendar(request):
    # Events still client-fetched from /data.jsonl (lift to Django for slug + base.html shell).
    return render(request, "tools/calendar.html")


def stem_comparison(request):
    return render(request, "tools/stem_comparison.html")


def gear_matrix(request):
    return render(request, "tools/gear_matrix.html")


def gearftp(request):
    return render(request, "tools/gearftp.html",
                  {"related_items": _related_items("tires", "helmets", "apparel", "lubes-cleaning")})
