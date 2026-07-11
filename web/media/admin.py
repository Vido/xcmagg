from django.contrib import admin
from django.utils.html import format_html

from nodes.admin import NodeAdmin
from .models import Photo


class PhotoInline(admin.TabularInline):
    model = Photo
    extra = 0
    readonly_fields = ('image_preview', 'created_at')
    fields = ('image', 'image_preview', 'created_at')  # show both

    def image_preview(self, obj):
        if obj.image:
            return format_html('<img src="{}" style="height: 75px;"/>', obj.image.url)
        return 'N/A'
    image_preview.short_description = 'Preview'


# Contribute the Photo inline to the Node admin without the spine importing media.
NodeAdmin.inlines = (*NodeAdmin.inlines, PhotoInline)


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
        "alt_text",
        "caption",
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