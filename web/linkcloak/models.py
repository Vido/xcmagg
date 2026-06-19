from urllib.parse import urlencode, urljoin, urlparse, urlunparse, parse_qsl

from django.conf import settings
from django.db import models
from django.urls import reverse
from django.utils.crypto import get_random_string
from django.utils.text import slugify


class Link(models.Model):
    """A cloakable outbound link.

    When ``cloak`` is set the public href points at the ``/go/<slug>/`` redirect
    so the destination can be changed (link-rot) and tracked without touching the
    pages that reference it. ``noopener noreferrer`` is always emitted; the SEO
    ``rel`` tokens are configurable.
    """

    target_url = models.URLField(max_length=2000)
    slug = models.SlugField(max_length=64, unique=True, blank=True)
    cloak = models.BooleanField(default=True)

    rel_sponsored = models.BooleanField(default=False)
    rel_nofollow = models.BooleanField(default=True)
    rel_ugc = models.BooleanField(default=False)

    # Optional UTM params appended to the destination (not the cloak path).
    utm_source = models.CharField(max_length=128, blank=True)
    utm_medium = models.CharField(max_length=128, blank=True)
    utm_campaign = models.CharField(max_length=128, blank=True)
    utm_term = models.CharField(max_length=128, blank=True)
    utm_content = models.CharField(max_length=128, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    # Optional hint for friendly slug generation; not persisted.
    slug_hint = ""

    def __str__(self):
        return f"{self.slug} -> {self.target_url}"

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = self._generate_slug()
        super().save(*args, **kwargs)

    def _generate_slug(self):
        base = slugify(self.slug_hint)[:40] if self.slug_hint else ""
        while True:
            suffix = get_random_string(6).lower()
            candidate = f"{base}-{suffix}" if base else suffix
            if not Link.objects.filter(slug=candidate).exists():
                return candidate

    @property
    def rel(self):
        tokens = ["noopener", "noreferrer"]
        if self.rel_sponsored:
            tokens.append("sponsored")
        if self.rel_nofollow:
            tokens.append("nofollow")
        if self.rel_ugc:
            tokens.append("ugc")
        return " ".join(tokens)

    def get_target_url(self):
        """Destination with any configured UTM params merged into the query.

        Existing query params are preserved; only the set utm_* keys are added
        (overriding a same-named existing param)."""
        utm = {
            "utm_source": self.utm_source,
            "utm_medium": self.utm_medium,
            "utm_campaign": self.utm_campaign,
            "utm_term": self.utm_term,
            "utm_content": self.utm_content,
        }
        utm = {k: v for k, v in utm.items() if v}
        if not utm:
            return self.target_url
        parts = urlparse(self.target_url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query.update(utm)
        return urlunparse(parts._replace(query=urlencode(query)))

    def get_redirect_url(self):
        if not self.cloak:
            return self.get_target_url()
        path = reverse("linkcloak:go", args=[self.slug])
        base = settings.LINKCLOAK_BASE_URL
        return urljoin(base, path) if base else path


class ClickEvent(models.Model):
    """Append-only click log for cloaked links. One row per /go/ hit."""

    link = models.ForeignKey(
        Link,
        on_delete=models.CASCADE,
        related_name="clicks",
    )
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True)
    referer = models.CharField(max_length=512, blank=True)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="link_clicks",
    )
    is_bot = models.BooleanField(default=False, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=["link", "created_at"]),
        ]
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.link.slug} @ {self.created_at:%Y-%m-%d %H:%M}"
