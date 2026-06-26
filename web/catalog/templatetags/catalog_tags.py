import bleach
import markdown
from django import template
from django.utils.safestring import mark_safe

register = template.Library()

_ALLOWED_TAGS = [
    "p", "br",
    "h1", "h2", "h3", "h4", "h5", "h6",
    "strong", "em", "del", "code", "pre",
    "ul", "ol", "li",
    "blockquote",
    "a",
    "table", "thead", "tbody", "tr", "th", "td",
    "hr",
]

_ALLOWED_ATTRS = {
    "a": ["href", "title", "rel"],
    "th": ["align"],
    "td": ["align"],
}


@register.filter(is_safe=True)
def render_md(value):
    if not value:
        return ""
    html = markdown.markdown(value, extensions=["fenced_code", "tables"])
    clean = bleach.clean(html, tags=_ALLOWED_TAGS, attributes=_ALLOWED_ATTRS, strip=True)
    return mark_safe(clean)
