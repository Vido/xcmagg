from django import template
from django.urls import reverse
from django.utils.http import urlencode
from django.shortcuts import resolve_url


register = template.Library()

@register.simple_tag(takes_context=True)
def url2(context, viewname, next=None, **kwargs):
    url = reverse(viewname, kwargs=kwargs)

    name = context.get("redirect_field_name", "next")
    value = next or context.get("redirect_field_value")

    if value:
        return f"{url}?{urlencode({name: value})}"

    return url