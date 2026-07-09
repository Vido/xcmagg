import asyncio
import ipaddress
import json
import os
import socket
import sys
import urllib.request
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(__file__))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.prod_settings")

import django

django.setup()

from decouple import config
from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

MCP_TOKEN = config("MCP_SECRET_TOKEN")


class BearerAuth(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if request.headers.get("Authorization") != f"Bearer {MCP_TOKEN}":
            return Response("Not Found", status_code=404)
        return await call_next(request)


mcp = FastMCP("xcmagg-catalog", streamable_http_path="/")


def _resolve_manufacturer(query: str):
    from catalog.models import Manufacturer

    return (
        Manufacturer.objects.filter(node__slug=query).first()
        or Manufacturer.objects.filter(node__title__iexact=query).first()
    )


def _resolve_category(query: str):
    from catalog.models import Category

    return (
        Category.objects.filter(node__slug=query).first()
        or Category.objects.filter(node__title__iexact=query).first()
    )


def _fetch_photo(node, image_url: str, is_primary: bool = True):
    from django.core.files.base import ContentFile
    from media.models import Photo

    parsed = urlparse(image_url)
    if parsed.scheme != "https":
        raise ValueError("image_url must be HTTPS")
    try:
        ip = socket.gethostbyname(parsed.hostname)
        addr = ipaddress.ip_address(ip)
        if addr.is_private or addr.is_loopback or addr.is_link_local:
            raise ValueError(f"image_url resolves to private/internal address: {ip}")
    except socket.gaierror:
        raise ValueError(f"Cannot resolve hostname: {parsed.hostname}")

    with urllib.request.urlopen(image_url, timeout=10) as resp:  # noqa: S310
        content = resp.read()

    filename = (parsed.path.split("/")[-1].split("?")[0] or "photo.jpg")[:100]
    photo = Photo(node=node, is_primary=is_primary)
    photo.image.save(filename, ContentFile(content))


@mcp.tool()
async def search_brands(query: str) -> list[dict]:
    """Search existing Manufacturers by name. Call this before create_catalog_item."""
    def _run():
        from catalog.models import Manufacturer
        results = Manufacturer.objects.filter(node__title__icontains=query).select_related("node")[:10]
        return [{"slug": m.node.slug, "title": m.node.title, "website": m.website} for m in results]
    return await asyncio.to_thread(_run)


@mcp.tool()
async def search_categories(query: str) -> list[dict]:
    """Search existing Categories by name. Call this before create_catalog_item."""
    def _run():
        from catalog.models import Category
        results = Category.objects.filter(node__title__icontains=query).select_related("node")[:10]
        return [{"slug": c.node.slug, "title": c.node.title} for c in results]
    return await asyncio.to_thread(_run)


@mcp.tool()
async def create_manufacturer(name: str, website: str = "") -> dict:
    """Create a new Manufacturer. Only call after the user confirms it should be created."""
    def _run():
        from django.core.exceptions import ValidationError
        from django.core.validators import URLValidator
        from catalog.models import Manufacturer
        from nodes.models import Node, NodeKind

        if website:
            try:
                URLValidator()(website)
            except ValidationError as exc:
                raise ValueError(f"Bad URL: {website}") from exc

        node, _ = Node.objects.update_or_create(
            title=name,
            kind=NodeKind.MANUFACTURER,
            defaults={},
        )
        Manufacturer.objects.update_or_create(node=node, defaults={"website": website})
        return {"slug": node.slug, "title": node.title}
    return await asyncio.to_thread(_run)


@mcp.tool()
async def create_catalog_item(
    title: str,
    manufacturer: str,
    category: str,
    description: str = "",
    links: list[dict] = [],
    image_url: str = "",
) -> dict:
    """Create or update a catalog item with affiliate links and optional primary photo.

    manufacturer: slug or title of an existing Manufacturer (use search_brands first).
    category: slug or title of an existing Category (use search_categories first).
    links: list of dicts with keys: text, url, is_affiliate (bool), promo_code (optional).
    image_url: HTTPS URL to product image — fetched server-side. Optional.
    """
    def _run():
        from django.contrib.auth import get_user_model
        from django.core.exceptions import ValidationError
        from django.core.validators import URLValidator
        from django.db import transaction
        from catalog.models import Item
        from catalog.services import RetailerLinkService
        from nodes.models import Node, NodeKind

        validate_url = URLValidator()
        for link in links:
            url = link.get("url", "")
            try:
                validate_url(url)
            except ValidationError as exc:
                raise ValueError(f"Bad link URL: {url}") from exc

        mfr = _resolve_manufacturer(manufacturer)
        if not mfr:
            raise ValueError(
                f"Manufacturer {manufacturer!r} not found. "
                "Use search_brands() to find it or create_manufacturer() to add it."
            )

        cat = _resolve_category(category)
        if not cat:
            raise ValueError(
                f"Category {category!r} not found. "
                "Use search_categories() to find it."
            )

        owner = get_user_model().objects.filter(is_superuser=True).first()
        if not owner:
            raise RuntimeError("No superuser found in database.")

        with transaction.atomic():
            node, _ = Node.objects.update_or_create(
                title=title,
                kind=NodeKind.CATALOG_ITEM,
                defaults={"owner": owner},
            )
            Item.objects.update_or_create(
                node=node,
                defaults={
                    "manufacturer": mfr,
                    "category": cat,
                    "description": description,
                },
            )
            RetailerLinkService.sync(node=node, links_json=json.dumps(links))

            if image_url:
                _fetch_photo(node, image_url, is_primary=True)

        item = node.item
        return {"url": item.get_absolute_url(), "shortcode": node.shortcode}
    return await asyncio.to_thread(_run)


@mcp.tool()
async def add_item_photo(shortcode: str, image_url: str, is_primary: bool = True) -> dict:
    """Download an image from a URL and attach it to an existing catalog item."""
    def _run():
        from nodes.models import Node, NodeKind
        node = Node.objects.get(shortcode=shortcode, kind=NodeKind.CATALOG_ITEM)
        _fetch_photo(node, image_url, is_primary=is_primary)
        return {"ok": True}
    return await asyncio.to_thread(_run)


async def _upload_photo(request: Request) -> Response:
    form = await request.form()
    shortcode = form.get("shortcode")
    is_primary = str(form.get("is_primary", "true")).lower() == "true"
    image_file = form.get("image")

    if not shortcode or not image_file:
        return JSONResponse({"error": "shortcode and image required"}, status_code=400)

    content = await image_file.read()
    filename = image_file.filename or "photo.jpg"

    def _run():
        from django.core.files.base import ContentFile
        from django.db import transaction
        from media.models import Photo
        from nodes.models import Node, NodeKind

        node = Node.objects.get(shortcode=shortcode, kind=NodeKind.CATALOG_ITEM)
        with transaction.atomic():
            if is_primary:
                Photo.objects.filter(node=node, is_primary=True).update(is_primary=False)
            photo = Photo(node=node, is_primary=is_primary)
            photo.image.save(filename, ContentFile(content))
        return {"ok": True, "photo_url": photo.image.url}

    result = await asyncio.to_thread(_run)
    return JSONResponse(result)


if __name__ == "__main__":
    import uvicorn

    app = mcp.streamable_http_app()
    app.add_route("/upload", _upload_photo, methods=["POST"])
    app.add_middleware(BearerAuth)
    uvicorn.run(app, host="0.0.0.0", port=8001)
