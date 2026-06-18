import re

from ipware import get_client_ip as _ipware_get_client_ip

from linkcloak.models import ClickEvent

# Compact, dependency-free bot signature. Matches the common crawlers, previews
# and CLI/library user agents that would otherwise pollute click analytics.
BOT_UA_RE = re.compile(
    r"bot|crawl|spider|slurp|mediapartners|bingpreview|facebookexternalhit|"
    r"whatsapp|telegrambot|headless|phantom|python-requests|httpx|aiohttp|"
    r"go-http|curl|wget|libwww|scrapy|monitor|uptime|preview|feedfetcher",
    re.IGNORECASE,
)


def get_client_ip(request):
    ip, _routable = _ipware_get_client_ip(request)
    return ip


def is_bot(user_agent):
    if not user_agent or not user_agent.strip():
        return True
    return bool(BOT_UA_RE.search(user_agent))


def record_click(request, link):
    """Log a single click for a cloaked Link. Never raises into the redirect."""
    user_agent = request.META.get("HTTP_USER_AGENT", "")
    return ClickEvent.objects.create(
        link=link,
        ip_address=get_client_ip(request),
        user_agent=user_agent,
        referer=request.META.get("HTTP_REFERER", "")[:512],
        user=request.user if request.user.is_authenticated else None,
        is_bot=is_bot(user_agent),
    )
