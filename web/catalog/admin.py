from django.contrib import admin
from django.utils.html import format_html

from catalog.models import Category, Manufacturer, Item, RetailerLink


# ---------- Category ----------

@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = (
        "node_title",
        "durability",
        "audience",
        "icon_preview",
        "node_visibility",
        "node_created_at",
    )
    list_filter = ("durability", "audience")
    search_fields = ("node__title", "node__shortcode")
    autocomplete_fields = ("node",)

    def node_title(self, obj):
        return obj.node.title
    node_title.short_description = "Title"

    def node_visibility(self, obj):
        return obj.node.visibility
    node_visibility.short_description = "Visibility"

    def node_created_at(self, obj):
        return obj.node.created_at
    node_created_at.short_description = "Created"

    def icon_preview(self, obj):
        return obj.icon or "-"
    icon_preview.short_description = "Icon"


# ---------- Manufacturer ----------

@admin.register(Manufacturer)
class ManufacturerAdmin(admin.ModelAdmin):
    list_display = (
        "node_title",
        "website",
        "icon_preview",
        "node_created_at",
    )
    search_fields = ("node__title", "node__shortcode", "website")
    autocomplete_fields = ("node",)

    def node_title(self, obj):
        return obj.node.title
    node_title.short_description = "Title"

    def node_created_at(self, obj):
        return obj.node.created_at
    node_created_at.short_description = "Created"

    def icon_preview(self, obj):
        return obj.icon or "-"
    icon_preview.short_description = "Icon"

@admin.register(Item)
class ItemAdmin(admin.ModelAdmin):
    list_display = (
        "display_name",
        "category",
        "manufacturer",
        "node_kind",
        "is_listed",
        "is_sold",
        "price",
        "node_created_at",
    )
    list_filter = (
        "node__kind",
        "is_listed",
        "is_sold",
        "category",
        "manufacturer",
    )
    autocomplete_fields = (
        "node",
        "category",
        "manufacturer",
        "catalog_node",
    )
    search_fields = (
        "node__title",
        "node__shortcode",
        "catalog_node__title",
    )
    readonly_fields = (
        "node",
        "display_name",
    )

    def node_kind(self, obj):
        return obj.node.kind
    node_kind.short_description = "Kind"

    def node_created_at(self, obj):
        return obj.node.created_at
    node_created_at.short_description = "Created"


# ---------- RetailerLink ----------

@admin.register(RetailerLink)
class RetailerLinkAdmin(admin.ModelAdmin):
    list_display = (
        "node",
        "label",
        "text",
        "is_affiliate",
        "promo_code",
        "target_url",
        "cloak",
    )
    list_filter = ("is_affiliate", "link__cloak")
    search_fields = ("text", "link__target_url", "link__slug", "node__title")
    raw_id_fields = ("node", "link")

    @admin.display(description="Destination")
    def target_url(self, obj):
        return obj.link.target_url

    @admin.display(boolean=True, description="Cloak")
    def cloak(self, obj):
        return obj.link.cloak
