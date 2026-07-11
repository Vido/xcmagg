from pathlib import Path

import duckdb
from django.conf import settings
from django.shortcuts import render
from django.utils.text import slugify

from nodes.models import Node, NodeKind
from catalog.selectors import CategorySelectors

_EVENTS_DB = Path(settings.DATA_DIR) / 'events.duckdb'


def _top_cities(limit=5):
    try:
        with duckdb.connect(str(_EVENTS_DB), read_only=True) as con:
            rows = con.execute("""
                SELECT location.city, location.uf, COUNT(*) AS cnt
                FROM schema_events
                WHERE TRY_CAST(date_range->>'start_date' AS DATE) > CURRENT_DATE
                  AND location.city IS NOT NULL
                GROUP BY 1, 2
                ORDER BY cnt DESC
                LIMIT ?
            """, [limit]).fetchall()
        return [{'city': r[0], 'uf': r[1], 'count': r[2], 'slug': slugify(r[0])} for r in rows]
    except Exception:
        return []


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
    return render(request, "tools/calendar.html", {'top_cities': _top_cities()})


def stem_comparison(request):
    return render(request, "tools/stem_comparison.html",
                  {"related_items": _related_items("bars-stems", "bar-tape-grips")})


def dropbar_stem_comparison(request):
    return render(request, "tools/dropbar_stem_comparison.html",
                  {"related_items": _related_items("bars-stems", "bar-tape-grips")})


def gear_matrix(request):
    return render(request, "tools/gear_matrix.html")


def gearftp(request):
    return render(request, "tools/gearftp.html",
                  {"related_items": _related_items("tires", "helmets", "apparel", "lubes-cleaning")})
