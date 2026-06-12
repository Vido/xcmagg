from django.shortcuts import render
from django.http import Http404
from django.shortcuts import render, get_object_or_404
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied

from nodes.models import Node, NodeKind
from engagement.selectors import PostSelector

from catalog.selectors import (
    ItemSelectors,
    CategorySelectors,
    ManufacturerSelectors,
    user_wishlist,
)

User = get_user_model()

def home(request):

    context =  {
        'highlighted_posts': PostSelector.highlighted()[:6],
        'highlighted_inventory': ItemSelectors.highlighted_inventory()[:5],
        'highlighted_catalog': ItemSelectors.highlighted_catalog()[:5],
        'categories': CategorySelectors.featured()[:6],
        'manufacturers': ManufacturerSelectors.featured()[:12],
    }

    return render(
        request,
        'home.html',
        context,
    )

def category_profile(request, slug):
    node = get_object_or_404(Node, slug=slug, kind=NodeKind.CATEGORY)
    return render(
        request,
        "catalog/node_profile.html",
        {
            "node": node,
            "inventory_items": CategorySelectors.inventory_items(node),
            "catalog_items": CategorySelectors.catalog_items(node),
            'community_posts': PostSelector.posts_under(node)
        },
    )

def manufacturer_profile(request, slug):
    node = get_object_or_404(Node, slug=slug, kind=NodeKind.MANUFACTURER)
    return render(
        request,
        "catalog/node_profile.html",
        {
            "node": node,
            "inventory_items": ManufacturerSelectors.inventory_items(node),
            "catalog_items": ManufacturerSelectors.catalog_items(node),
            'community_posts': PostSelector.posts_under(node)
        },
    )

def catalog_details(request, brand, shortcode, slug):

    node = get_object_or_404(Node, shortcode=shortcode, kind=NodeKind.CATALOG_ITEM)

    if slug != node.slug:
        return redirect(
            reverse("catalog-details", args=[brand, shortcode, slug]),
            permanent=True,
        )

    return render(
        request,
        "catalog/item_detail.html",
        {
            "owner": node.item.manufacturer,
            "item": node.item,
        },
    )

def inventory_page(request, username):
    owner = get_object_or_404(User, username=username)
    inventory_qs = ItemSelectors.visible_inventory_for(
        viewer=request.user,
        owner=owner,
    )
    pocket_dump_qs = PostSelector.visible_posts_for(
        viewer=request.user,
        owner=owner,
        order_by='-node__published_at',
    )

    reviews_qs = PostSelector.posts_under_parent(
        [
            NodeKind.CATALOG_ITEM,
            NodeKind.INVENTORY_ITEM,
            NodeKind.MANUFACTURER
        ]
    )
    return render(
        request,
        "catalog/user_inventory.html",
        #"catalog/node_profile.html",
        {
            'owner': owner,
            #'items_qs': inventory_qs, # TODO: Remove
            'inventory_items': inventory_qs[:6],
            'community_posts': pocket_dump_qs[:3],
            'reviews_posts': reviews_qs[:3],
        },
    )

def inventory_details(request, username, shortcode, slug):
    owner = get_object_or_404(User, username=username)

    item = ItemSelectors.visible_item_for(
        viewer=request.user,
        owner=owner,
        shortcode=shortcode,
    )

    if item is None:
        raise PermissionDenied

    return render(
        request,
        "catalog/item_detail.html",
        {
            "owner": owner,
            "item": item,
        },
    )


def wishlist_page(request, username):
    owner = get_object_or_404(User, username=username)

    items = user_wishlist(owner)

    return render(
        request,
        "inventory/wishlist_page.html",
        {
            "owner": owner,
            "items": items,
        },
    )
