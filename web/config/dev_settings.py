from decouple import config

from .base_settings import *

# SECURITY WARNING: keep the secret key used in production secret!
dev_placeholder = 'django-insecure-4+w&bfoe_2&0#sufboe8-$6!o=e33w0dy_^%-u2d678$g&^iu^'
SECRET_KEY = config('SECRET_KEY', default=dev_placeholder)

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['*']
INTERNAL_IPS = ['127.0.0.1']

CACHES['default'] = {
    'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',  # DEV - not PROD!
    'LOCATION': 'magiclink-tokens',
    'OPTIONS': {
        'MAX_ENTRIES': 1000,
    },
}

# https://developers.cloudflare.com/turnstile/troubleshooting/testing/
TURNSTILE_SITE_KEY = config('TURNSTILE_SITE_KEY', default='1x00000000000000000000AA')
TURNSTILE_SECRET = config('TURNSTILE_SECRET_KEY', default='1x0000000000000000000000000000000AA') # Allows
#TURNSTILE_SECRET = config('TURNSTILE_SECRET_KEY', default='2x0000000000000000000000000000000AA') # Fails

EMAIL_BACKEND = (
    "django.core.mail.backends.console.EmailBackend"
)
