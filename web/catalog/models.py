from django.db import models
from django.urls import reverse
from django.utils.functional import cached_property 
from django.core.exceptions import ValidationError

from nodes.models import NodeKind, NodeBoundModel


class Category(NodeBoundModel, models.Model):

    node = models.OneToOneField(
        'nodes.Node',
        on_delete=models.CASCADE,
        related_name="category",
        limit_choices_to={"kind": NodeKind.CATEGORY},
    )

    icon = models.TextField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        verbose_name_plural = "categories"

    def __str__(self):
        return self.title

    def get_absolute_url(self):
        return reverse(
            'category-profile',
            kwargs={'slug': self.slug},
        )


class Manufacturer(NodeBoundModel, models.Model):
    
    node = models.OneToOneField(
        'nodes.Node',
        on_delete=models.CASCADE,
        related_name='manufacturer',
        limit_choices_to={'kind': NodeKind.MANUFACTURER},
    )

    icon = models.TextField(blank=True, null=True)
    website = models.URLField(blank=True)

    def __str__(self):
        return self.title

    def get_absolute_url(self):
        return reverse(
            'manufacturer-profile',
            kwargs={"slug": self.slug},
        )


class Item(NodeBoundModel, models.Model):

    node = models.OneToOneField(
        'nodes.Node',
        on_delete=models.CASCADE,
        related_name="item",
        limit_choices_to={"kind__in": 
            [NodeKind.CATALOG_ITEM, NodeKind.INVENTORY_ITEM]
        }
    )

    # Shared fields
    description = models.TextField(null=True, blank=True)

    category = models.ForeignKey(
        Category,
        on_delete=models.PROTECT,
        related_name="items",
        null=True,
        blank=True,
    )

    manufacturer = models.ForeignKey(
        Manufacturer,
        on_delete=models.PROTECT,
        related_name="items",
        null=True,
        blank=True,
    )

    # Reference to canonical Node (nullable for fully custom UGC items)
    catalog_node = models.ForeignKey(
        'nodes.Node',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        limit_choices_to={'kind': NodeKind.CATALOG_ITEM},
        related_name='ugc_items'
    )

    # Marketplace fields (optional, only meaningful for UGC)
    acquired_at = models.DateField(null=True, blank=True)
    cost = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    is_listed = models.BooleanField(default=False)
    accepts_trade = models.BooleanField(default=False)
    price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    is_sold = models.BooleanField(default=False)
    sold_at = models.DateTimeField(null=True, blank=True)


    def __str__(self):
        parts = [f'{self.kind} #{self.pk}']

        if hasattr(self, 'manufacturer') and self.manufacturer:
            parts.append(str(self.manufacturer))

        if self.title:
            parts.append(self.title)

        return " ".join(parts)

    def get_absolute_url(self):

        if self.kind == NodeKind.INVENTORY_ITEM:
            return reverse(
                "inventory-details",
                kwargs={
                    "username": self.owner.username,
                    "shortcode": self.shortcode,
                    "slug": self.slug,
                },
            )

        if self.kind == NodeKind.CATALOG_ITEM:
            return reverse(
                "catalog-details",
                kwargs={
                    "brand": self.manufacturer.slug,
                    "shortcode": self.shortcode,
                    "slug": self.slug,
                },
            )

        raise ValueError(f"Unsupported node kind: {self.kind}")

    def get_edit_url(self):
        return reverse('edit-item',
            kwargs={
                "shortcode": self.shortcode,
                "slug": self.slug,
            }
        )

    @cached_property
    def is_catalog(self):
        return self.kind == NodeKind.CATALOG_ITEM
        
    @property
    def display_name(self):
        # Use the UGC Node's title if set
        if self.node.title:
            return self.node.title
        # Fallback to the catalog item's title if available
        if self.catalog_node:
            return self.catalog_node.title
        # Final fallback: Node ID or generic placeholder
        return f"Item {self.node.id}"


class RetailerLink(models.Model):

    node = models.ForeignKey(
        "nodes.Node",
        on_delete=models.CASCADE,
        related_name="retailer_links",
        limit_choices_to={'kind__in': [
            NodeKind.CATALOG_ITEM,
            NodeKind.INVENTORY_ITEM,
        ]
        },

    )

    label = models.CharField(max_length=32)  # "Knife", "Pen", "Watch"
    text = models.CharField(max_length=64)

    # Destination, slug, cloak and rel= live on the cloakable Link.
    link = models.OneToOneField(
        "linkcloak.Link",
        on_delete=models.CASCADE,
        related_name="retailer_link",
    )
    order = models.PositiveIntegerField(default=0)

    is_affiliate = models.BooleanField(default=False)
    promo_code = models.CharField(max_length=32, blank=True)

    class Meta:
        #unique_together = ("node", "url")
        ordering = ["order"]

    def __str__(self):
        return f"{self.text} ({'affiliate' if self.is_affiliate else 'direct'})"

    # Proxies to the underlying Link, so templates stay simple.
    @property
    def target_url(self):
        return self.link.target_url

    @property
    def rel(self):
        return self.link.rel

    def get_redirect_url(self):
        return self.link.get_redirect_url()