from django import template
from markdown_it import MarkdownIt
import bleach

register = template.Library()
md = MarkdownIt("commonmark")

ALLOWED_TAGS = list(bleach.sanitizer.ALLOWED_TAGS) + [
    "p", "pre", "code", "blockquote",
    "h1", "h2", "h3", "h4",
    "ul", "ol", "li", "strong", "em", "a",
]


@register.filter
def markdown(text):
    html = md.render(text or "")
    return bleach.clean(html, tags=ALLOWED_TAGS, strip=True)