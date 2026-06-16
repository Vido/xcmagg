from decouple import config

from .base_settings import *

SECRET_KEY = config('SECRET_KEY')

DEBUG = False

ALLOWED_HOSTS = config(
    "ALLOWED_HOSTS",
    default="racefeed.com.br,www.racefeed.com.br",
    cast=lambda v: v.split(","),
)

# Behind the lvido-proxy nginx (TLS terminated there). proxy.conf forwards
# X-Forwarded-Proto, so Django sees the request as secure.
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

# Required for HTTPS POST (admin/login) — Django checks Origin against this list.
CSRF_TRUSTED_ORIGINS = config(
    "CSRF_TRUSTED_ORIGINS",
    default="https://racefeed.com.br,https://www.racefeed.com.br",
    cast=lambda v: v.split(","),
)

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.memcached.PyMemcacheCache",
        # Defaults to the memcached service defined in infra/docker-compose.yaml
        "LOCATION": config("MEMCACHED_LOCATION", default="memcached:11211"),
    }
}

TURNSTILE_SITE_KEY = config('TURNSTILE_SITE_KEY')
TURNSTILE_SECRET = config('TURNSTILE_SECRET_KEY')

EMAIL_BACKEND = "anymail.backends.postmark.EmailBackend"
ANYMAIL = {
    "POSTMARK_SERVER_TOKEN": config('POSTMARK_SERVER_TOKEN', default='missing'),
    "SEND_DEFAULTS": {
        "esp_extra": {"MessageStream": "outbound"},
    },
}

# TODO: R2 STORAGE
