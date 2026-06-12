from django.db.models import Count, Q

from nodes.models import Visibility
from catalog.models import Category, Manufacturer, Item

from django.db.models import Q

from nodes.models import Node, NodeKind, Visibility


def _featured_feed(*, model):
    return (
        model.objects
        .annotate(
            item_count=Count(
                "node__children",
                filter=Q(
                    node__children__visibility=Visibility.PUBLIC,
                    node__children__kind__in=[
                        NodeKind.INVENTORY_ITEM,
                        NodeKind.CATALOG_ITEM,
                    ],
                    node__children__item__is_sold=False,
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
                node__kind=NodeKind.CATALOG_ITEM
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
            .filter(node__owner=owner)
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