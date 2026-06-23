"""Dynamic sitemap for racefeed.com.br.

Replaces the hand-maintained public/sitemap.xml. Covers the Django app pages
(home, list pages, catalog categories/manufacturers/items) plus the static tool
pages still served by nginx. Domain is pinned (the DB Site defaults to
example.com), so URLs are correct regardless of the Sites row.
"""

from django.contrib.sitemaps import Sitemap
from django.urls import reverse

DOMAIN = "racefeed.com.br"


class _FixedSite:
    domain = DOMAIN
    name = "RaceFeed"


class BaseSitemap(Sitemap):
    protocol = "https"

    # Ignore the request/DB Site; always emit the production domain.
    def get_urls(self, page=1, site=None, protocol=None):
        return super().get_urls(page=page, site=_FixedSite(), protocol=protocol)


class StaticViewSitemap(BaseSitemap):
    """Django-rendered top-level pages, addressed by URL name."""
    priority = 0.8
    changefreq = "weekly"

    def items(self):
        return [
            "home",
            "events:calendar",
            "tools:nutrition-calculator",
            "tools:hydration-calculator",
            "tools:fuel-plan",
            "tools:stem-comparison",
            "tools:gear-matrix",
            "category-list",
            "manufacturer-list",
            "catalog-list",
        ]

    def location(self, name):
        return reverse(name)


class StaticPageSitemap(BaseSitemap):
    """Tool pages still served as static HTML by nginx (no Django route)."""
    priority = 0.7
    changefreq = "monthly"

    def items(self):
        return [
            "/gearftp.html",
        ]

    def location(self, path):
        return path


class CategorySitemap(BaseSitemap):
    priority = 0.6
    changefreq = "weekly"

    def items(self):
        from catalog.selectors import CategorySelectors
        return list(CategorySelectors.featured())
    # uses Category.get_absolute_url()


class ManufacturerSitemap(BaseSitemap):
    priority = 0.6
    changefreq = "weekly"

    def items(self):
        from catalog.selectors import ManufacturerSelectors
        return list(ManufacturerSelectors.featured())
    # uses Manufacturer.get_absolute_url()


class CatalogItemSitemap(BaseSitemap):
    priority = 0.5
    changefreq = "weekly"

    def items(self):
        from catalog.selectors import ItemSelectors
        return list(ItemSelectors.highlighted_catalog())
    # uses Item.get_absolute_url()


class BlogSitemap(BaseSitemap):
    """Markdown blog posts (file-backed, both languages)."""
    priority = 0.7
    changefreq = "weekly"

    def items(self):
        from blog.loader import LANGS, list_posts
        return [p for lang in LANGS for p in list_posts(lang)]

    def location(self, post):
        return reverse("blog-article", args=[post.lang, post.slug])

    def lastmod(self, post):
        return post.updated_date or post.publish_date


SITEMAPS = {
    "static": StaticViewSitemap,
    "pages": StaticPageSitemap,
    "categories": CategorySitemap,
    "manufacturers": ManufacturerSitemap,
    "catalog": CatalogItemSitemap,
    "blog": BlogSitemap,
}
