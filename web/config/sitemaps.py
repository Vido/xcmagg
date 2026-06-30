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

    def get_urls(self, page=1, site=None, protocol=None):
        urls = super().get_urls(page=page, site=_FixedSite(), protocol=protocol)
        for url_info in urls:
            url_info['images'] = self.get_images(url_info['item'])
        return urls

    def get_images(self, item):
        return []


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
            "tools:gearftp",
            "category-list",
            "manufacturer-list",
            "catalog-list",
        ]

    def location(self, name):
        return reverse(name)


class CategorySitemap(BaseSitemap):
    priority = 0.6
    changefreq = "weekly"

    def items(self):
        from catalog.selectors import CategorySelectors
        return list(CategorySelectors.featured())

    def lastmod(self, obj):
        return obj.node.updated_at


class ManufacturerSitemap(BaseSitemap):
    priority = 0.6
    changefreq = "weekly"

    def items(self):
        from catalog.selectors import ManufacturerSelectors
        return list(ManufacturerSelectors.featured())

    def lastmod(self, obj):
        return obj.node.updated_at


class CatalogItemSitemap(BaseSitemap):
    priority = 0.5
    changefreq = "weekly"

    def items(self):
        from catalog.selectors import ItemSelectors
        from catalog.models import Item
        qs = ItemSelectors.highlighted_catalog()
        if hasattr(qs, 'prefetch_related'):
            qs = qs.prefetch_related('node__photos')
        return list(qs)

    def lastmod(self, obj):
        return obj.node.updated_at

    def get_images(self, item):
        return [
            {
                'loc': f"https://{DOMAIN}{photo.image.url}",
                'caption': item.node.title or '',
            }
            for photo in item.node.photos.all()
        ]


class PostSitemap(BaseSitemap):
    priority = 0.6
    changefreq = "weekly"

    def items(self):
        from engagement.models import Post
        return list(
            Post.visible.all()
            .select_related('node')
            .prefetch_related('node__photos')
            .order_by('-node__published_at')
        )

    def lastmod(self, post):
        return post.node.published_at

    def get_images(self, post):
        return [
            {
                'loc': f"https://{DOMAIN}{photo.image.url}",
                'caption': post.node.title or '',
            }
            for photo in post.node.photos.all()
        ]


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
    "categories": CategorySitemap,
    "manufacturers": ManufacturerSitemap,
    "catalog": CatalogItemSitemap,
    "posts": PostSitemap,
    "blog": BlogSitemap,
}
