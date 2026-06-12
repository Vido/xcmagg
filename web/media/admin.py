from django.contrib import admin
from django.utils.html import format_html
from .models import Photo


@admin.register(Photo)
class PhotoAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "node",
        "primary_thumb",
        "is_primary",
        "order",
        "created_at",
    )
    list_filter = ("node", "is_primary", "created_at")
    search_fields = ("node__title", "id")
    readonly_fields = ("created_at", "primary_thumb")
    ordering = ("node", "-is_primary", "order", "created_at")

    fields = (
        "node",
        "image",
        "primary_thumb",
        "is_primary",
        "order",
        "created_at",
    )

    def primary_thumb(self, obj):
        if obj.image:
            return format_html(
                '<img src="{}" style="height: 60px; border-radius: 4px;" />', obj.image.url
            )
        return "-"
        
    primary_thumb.short_description = "Preview"