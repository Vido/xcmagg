# admin.py

from django.contrib import admin

from .models import Post, Vote


@admin.register(Vote)
class VoteAdmin(admin.ModelAdmin):
    list_display = ("id", "node", "owner", "value", "created_at")
    list_filter = ("value", "created_at")
    search_fields = ("owner__username",)
    autocomplete_fields = ("node", "owner")
    readonly_fields = ("created_at",)
    ordering = ("-created_at",)


@admin.register(Post)
class PostAdmin(admin.ModelAdmin):
    list_display = (
        "slug",
        "node",
        "owner",
        "reply_to",
        "depth",
        "thread",
        "short_body",
    )
    list_filter = ("depth",)
    search_fields = ("body", "node__owner__username")
    autocomplete_fields = ("node", "reply_to")
    readonly_fields = ("depth", "thread")
    ordering = ("thread",)

    def owner(self, obj):
        return obj.node.owner

    owner.admin_order_field = "node__owner"
    owner.short_description = "Owner"

    def slug(self, obj):
        return obj.node.slug

    def short_body(self, obj):
        return (obj.body[:75] + "...") if len(obj.body) > 75 else obj.body

    short_body.short_description = "Body"
