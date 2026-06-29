import logging
from dataclasses import dataclass
from functools import lru_cache

from django.conf import settings

logger = logging.getLogger(__name__)

_GEO_DB = "GeoLite2-City.mmdb"


@dataclass(frozen=True)
class GeoInfo:
    country_code: str | None = None
    city: str | None = None
    lat: float | None = None
    lon: float | None = None
    timezone: str | None = None


@lru_cache(maxsize=None)
def _db_reader(mmdbfile):
    import geoip2.database
    return geoip2.database.Reader(mmdbfile)


def geoip_lookup(ip: str) -> dict:
    fn = settings.DATA_DIR / _GEO_DB
    if not fn.exists():
        return {}
    try:
        r = _db_reader(fn).city(ip)
    except Exception:
        logger.debug("GeoIP lookup failed for %s", ip, exc_info=True)
        return {}
    return {
        "city": r.city.name,
        "lat": r.location.latitude,
        "lon": r.location.longitude,
        "timezone": r.location.time_zone,
    }


class GeoMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        data = {}
        cc = request.META.get("HTTP_CF_IPCOUNTRY", "") or settings.FALLBACK_COUNTRY
        if cc.replace("XX", ""):
            data["country_code"] = cc
        if ip := request.META.get("HTTP_CF_CONNECTING_IP"):
            data.update(geoip_lookup(ip))
        request.geo = GeoInfo(**data)
        return self.get_response(request)