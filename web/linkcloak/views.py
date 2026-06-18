from django.shortcuts import get_object_or_404, redirect
from django.views.decorators.http import require_http_methods
from django_ratelimit.decorators import ratelimit

from linkcloak.clicks import record_click
from linkcloak.models import Link


@ratelimit(key="ip", rate="60/m", method="GET", block=True)
@require_http_methods(["GET"])
def go_link(request, slug):
    link = get_object_or_404(Link, slug=slug, cloak=True)
    record_click(request, link)
    return redirect(link.target_url)
