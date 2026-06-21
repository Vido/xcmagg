from django.shortcuts import render

from nodes.models import Node, NodeKind
from catalog.selectors import CategorySelectors


def fuel_calculator(request):
    nodes = Node.objects.filter(
        slug__in=["nutrition", "hydration"], kind=NodeKind.CATEGORY
    )
    related_items = []
    for node in nodes:
        related_items += list(CategorySelectors.catalog_items(node)[:4])
    return render(request, "tools/fuel_calculator.html", {"related_items": related_items})
