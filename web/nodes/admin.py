from django.contrib import admin

from nodes.models import Node


@admin.register(Node)
class NodeAdmin(admin.ModelAdmin):
    list_display = (
        'shortcode',
        'slug',
        'kind',
        'visibility',
        'created_at',
    )
    list_filter = ('kind', 'visibility')
    search_fields = ('title', 'shortcode')
    readonly_fields = ('shortcode', 'created_at', 'updated_at')

    # Node-attached inlines are contributed by the apps that own the related
    # models (media -> PhotoInline, engagement -> PostInline, catalog ->
    # RetailerLinkInline) by appending to `NodeAdmin.inlines` in their own
    # admin.py. The spine imports no domain apps.
    inlines = ()
