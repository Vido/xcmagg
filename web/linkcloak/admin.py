import csv

from django.contrib import admin
from django.db.models import Count, Q
from django.http import HttpResponse
from django.urls import reverse
from django.utils.html import format_html

from linkcloak.models import ClickEvent, Link


def _export_click_events_csv(modeladmin, request, queryset):
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="click_events.csv"'
    writer = csv.writer(response)
    writer.writerow(["slug", "destination", "ip_address", "date"])
    for row in queryset.values_list("link__slug", "link__target_url", "ip_address", "created_at"):
        writer.writerow(row)
    return response


_export_click_events_csv.short_description = "Export selected as CSV"


@admin.register(Link)
class LinkAdmin(admin.ModelAdmin):
    list_display = (
        "slug",
        "target_url",
        "cloak",
        "human_clicks",
        "rel_sponsored",
        "rel_nofollow",
        "rel_ugc",
        "created_at",
    )
    list_editable = ("target_url", "cloak")
    list_filter = ("cloak", "rel_sponsored", "rel_nofollow", "rel_ugc")
    search_fields = ("slug", "target_url")
    readonly_fields = ("created_at", "cloak_url")

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.annotate(
            _human_clicks=Count("clicks", filter=Q(clicks__is_bot=False)),
        )

    @admin.display(description="Clicks (human)", ordering="_human_clicks")
    def human_clicks(self, obj):
        return obj._human_clicks

    @admin.display(description="Cloak URL")
    def cloak_url(self, obj):
        if not obj.slug:
            return "-"
        url = reverse("linkcloak:go", args=[obj.slug])
        return format_html('<a href="{}" target="_blank">{}</a>', url, url)


@admin.register(ClickEvent)
class ClickEventAdmin(admin.ModelAdmin):
    actions = [_export_click_events_csv]
    list_display = (
        "link",
        "created_at",
        "ip_address",
        "is_bot",
        "user",
        "user_agent",
    )
    list_filter = ("is_bot", "created_at")
    search_fields = ("link__slug", "ip_address", "user_agent")
    date_hierarchy = "created_at"
    readonly_fields = (
        "link",
        "created_at",
        "ip_address",
        "user_agent",
        "referer",
        "user",
        "is_bot",
    )

    def has_add_permission(self, request):
        return False
