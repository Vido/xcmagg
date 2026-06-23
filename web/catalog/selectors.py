from django.db.models import Count, Q

from nodes.models import Visibility
from catalog.models import Category, Manufacturer, Item, Durability, Audience

from django.db.models import Q

from nodes.models import Node, NodeKind, Visibility


def _featured_feed(*, model):
    return (
        model.objects
        .annotate(
            item_count=Count(
                "items",
                filter=Q(
                    items__node__visibility=Visibility.PUBLIC,
                    items__node__kind__in=[
                        NodeKind.INVENTORY_ITEM,
                        NodeKind.CATALOG_ITEM,
                    ],
                    items__is_sold=False,
                ),
                distinct=True,
            )
        )
        .order_by("-item_count", "node__title")
    )

class CategorySelectors:
    inventory_items = lambda node: ItemSelectors.for_node(
        node=node,
        kind=NodeKind.INVENTORY_ITEM,
        field="category",
    )
    catalog_items = lambda node: ItemSelectors.for_node(
        node=node,
        kind=NodeKind.CATALOG_ITEM,
        field="category",
    )

    @staticmethod
    def featured():
        return _featured_feed(model=Category)

    @staticmethod
    def shelves():
        """Featured categories split into ordered shelves — rider consumables
        first, then bike consumables, then durables. Within each shelf the
        most-stocked categories surface first (via _featured_feed ordering)."""
        cats = list(_featured_feed(model=Category))  # evaluate once; reuse item_count
        shelves = [
            ("Showroom", Audience.BIKE, Durability.DURABLE),
            ("Fuel Your Ride", Audience.RIDER, Durability.CONSUMABLE),
            ("Keep It Rolling", Audience.PARTS, Durability.CONSUMABLE),
            ("Upgrade Your Bike", Audience.PARTS, Durability.DURABLE),
            ("Gear Up", Audience.RIDER, Durability.DURABLE),
        ]
        result = []
        for label, audience, durability in shelves:
            members = [
                c for c in cats
                if c.audience == audience and c.durability == durability
            ]
            if members:
                result.append({"label": label, "categories": members})

        shelved = {c.pk for shelf in result for c in shelf["categories"]}
        rest = [c for c in cats if c.pk not in shelved]
        if rest:
            result.append({"label": "More", "categories": rest})

        return result

class ManufacturerSelectors:
    inventory_items = lambda node: ItemSelectors.for_node(
        node=node,
        kind=NodeKind.INVENTORY_ITEM,
        field="manufacturer",
    )
    catalog_items = lambda node: ItemSelectors.for_node(
        node=node,
        kind=NodeKind.CATALOG_ITEM,
        field="manufacturer",
    )

    def featured():
        return _featured_feed(model=Manufacturer)


class ItemSelectors:

    @staticmethod
    def for_node(*, node, kind, field):
        return (
            Item.visible.public().filter(
                **{field: getattr(node, field)},
                node__kind=kind,
            )
            .select_related("node", "node__owner")
            .order_by("-node__published_at")
        )

    @staticmethod
    def highlighted_catalog():
        """
        Return the most recent public, unsold inventory items.
        """
        owners_count=Count(
            "node__ugc_items",
            filter=Q(
                node__ugc_items__node__kind=NodeKind.INVENTORY_ITEM
            ),
            distinct=True,
        )
        return (
            Item.visible.public().annotate(
                owners_count = owners_count
            )
            .filter(
                is_sold=False,
                node__kind=NodeKind.CATALOG_ITEM,
                manufacturer__isnull=False,
            )
            .select_related('node', 'node__owner', 'category', 'manufacturer')
            .order_by('-node__published_at')
        )

    @staticmethod
    def highlighted_inventory():
        return (
            Item.visible.public()
            .filter(
                is_sold=False,
                node__kind=NodeKind.INVENTORY_ITEM
            )
            .select_related('node', 'node__owner', 'catalog_node',
                'category', 'manufacturer')
            .order_by('-node__published_at')
        )
    @staticmethod
    def visible_inventory_for(viewer, owner):
        return (
            Item.visible.to(viewer, owner)
            .filter(node__owner=owner, node__kind=NodeKind.INVENTORY_ITEM)
            .select_related('node', 'node__owner', 'catalog_node')
            .order_by('-node__published_at')
        )

    @staticmethod
    def visible_item_for(*, viewer, owner, shortcode):
        return (
            Item.visible.to(viewer, owner)
            .filter(
                node__owner=owner,
                node__shortcode=shortcode,
                node__kind=NodeKind.INVENTORY_ITEM,
                is_sold=False,
            )
            .select_related("node", "node__owner", "catalog_node")
            .first()
        )

def user_wishlist(user):
    return (
        WishlistItem.objects
        .filter(user=user)
        .select_related("item", "item__category", "item__manufacturer")
        .order_by("-created_at")
    )