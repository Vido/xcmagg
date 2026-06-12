from django import template
from django.utils.safestring import mark_safe
from django.conf import settings

register = template.Library()

@register.simple_tag
def turnstile(size='normal', theme='auto'):
    size = size if size in ('normal', 'flexible', 'compact') else 'normal'
    data_size = {
     'normal': '',
     'flexible': 'data-size="flexible"',
     'compact': 'data-size="compact"',
    }[size]
    data_theme = theme if theme in ('auto', 'light', 'dark') else 'auto'
    data_sitekey = f'data-sitekey="{settings.TURNSTILE_SITE_KEY}"'
    html = f'<div class="cf-turnstile" {data_sitekey} {data_size} {data_theme}></div>'
    return mark_safe(html)
