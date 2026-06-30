from django.shortcuts import render
from django.http import Http404
from django.shortcuts import render, get_object_or_404
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied

from nodes.models import Node, NodeKind
from engagement.selectors import PostSelector, RatingSelector

from catalog.selectors import (
    ItemSelectors,
    CategorySelectors,
    ManufacturerSelectors,
    user_wishlist,
)

User = get_user_model()

def home(request):

    def trim(items, multiple):
        return items[:len(items) - len(items) % multiple]

    posts = trim(list(PostSelector.highlighted()[:6]), 3)
    inventory = trim(list(ItemSelectors.highlighted_inventory()[:8]), 4)
    catalog = trim(list(ItemSelectors.highlighted_catalog()[:8]), 4)

    context =  {
        'highlighted_posts': posts,
        'highlighted_inventory': inventory,
        'highlighted_catalog': catalog,
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

def category_list(request):
    return render(
        request,
        "catalog/list.html",
        {
            "list_title": "Browse by Category",
            "list_subtitle": "Explore gear organized by type",
            "shelves": CategorySelectors.shelves(),
            "card_template": "_category_card.html",
            "grid_class": "grid-cols-2 md:grid-cols-3 lg:grid-cols-6",
            "show_community": True,
            "highlighted_posts": PostSelector.posts_under_parent(
                [NodeKind.CATEGORY]
            ).order_by("-node__published_at")[:6],
            "empty_title": "No categories yet",
            "empty_text": "Start cataloging your gear and categories will appear here.",
        },
    )


def manufacturer_list(request):
    return render(
        request,
        "catalog/list.html",
        {
            "list_title": "Manufacturers",
            "list_subtitle": "Gear from trusted manufacturers",
            "objects": ManufacturerSelectors.featured(),
            "card_template": "_brand_card.html",
            "grid_class": "grid-cols-2 md:grid-cols-4 lg:grid-cols-6",
            "show_community": True,
            "highlighted_posts": PostSelector.posts_under_parent(
                [NodeKind.MANUFACTURER]
            ).order_by("-node__published_at")[:6],
            "empty_title": "No manufacturers yet",
            "empty_text": "Add gear and the manufacturers behind it will show up here.",
        },
    )


def catalog_list(request):
    return render(
        request,
        "catalog/list.html",
        {
            "list_title": "Catalog",
            "list_subtitle": "Browse gear from the catalog",
            "shelves": ItemSelectors.catalog_shelves(),
            "card_template": "_item_card.html",
            "grid_class": "grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5",
            "empty_title": "No gear in the catalog yet",
            "empty_text": "Be the first to add a piece of gear to the catalog.",
        },
    )


def catalog_details(request, brand, shortcode, slug):

    node = get_object_or_404(Node, shortcode=shortcode, kind=NodeKind.CATALOG_ITEM)

    if slug != node.slug:
        return redirect(
            reverse("catalog-details", args=[brand, shortcode, slug]),
            permanent=True,
        )

    reviews = (
        PostSelector.posts_under(node)
        .annotate(author_rating=RatingSelector.author_rating_subquery())
        .order_by("-node__published_at")
    )
    agg = RatingSelector.aggregate_for(node)

    return render(
        request,
        "catalog/item_detail.html",
        {
            "owner": node.item.manufacturer,
            "item": node.item,
            "reviews_posts": reviews,
            "rating_avg": agg["avg"],
            "rating_count": agg["count"],
            "user_rating": RatingSelector.user_rating(node, request.user),
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

    reviews_qs = PostSelector.visible_reviews_for(
        viewer=request.user,
        owner=owner,
        order_by='-node__published_at',
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
