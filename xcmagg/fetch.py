"""
fetch.py — product page scraper and photo uploader for the xcmagg catalog.

Subcommands:
    scrape <url>                    Fetch product metadata (title, brand, image_url, ...).
                                    Outputs JSON to stdout. No image bytes in output.
    upload <shortcode> <image-url>  Fetch image locally (residential IP) and POST directly
                                    to the MCP server upload endpoint. Reads MCP URL + token
                                    from .mcp.json at project root.

Usage (from xcmagg/ scraper dir via uv):
    uv run python fetch.py scrape "https://..."
    uv run python fetch.py upload Aks1 "https://..."
"""

import json
import sys
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from curl_cffi import requests as cf_requests


# ---------------------------------------------------------------------------
# Core fetch helpers (reusable by other scrapers)
# ---------------------------------------------------------------------------

def cf_session(impersonate: str = "chrome120") -> cf_requests.Session:
    return cf_requests.Session(impersonate=impersonate)


def fetch_page(url: str) -> BeautifulSoup:
    resp = cf_session().get(url, timeout=20, allow_redirects=True)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


def fetch_image(url: str) -> tuple[bytes, str]:
    resp = cf_session().get(url, timeout=20, allow_redirects=True)
    resp.raise_for_status()
    path = urlparse(url).path
    filename = (path.split("/")[-1].split("?")[0] or "photo.jpg")[:100]
    return resp.content, filename


# ---------------------------------------------------------------------------
# Retailer name from URL
# ---------------------------------------------------------------------------

_RETAILER_NAMES = {
    "mercadolivre.com.br": "Mercado Livre",
    "mercadolibre.com": "Mercado Libre",
    "amazon.com.br": "Amazon",
    "amazon.com": "Amazon",
    "shopee.com.br": "Shopee",
    "aliexpress.com": "AliExpress",
    "magazineluiza.com.br": "Magazine Luiza",
    "americanas.com.br": "Americanas",
    "submarino.com.br": "Submarino",
}


_AFFILIATE_PARAMS = {
    # AliExpress / Admitad
    "aff_platform", "aff_trace_key", "aff_fcid", "aff_item_seq",
    # Amazon Associates
    "tag", "linkcode", "linkid",
    # Mercado Livre / Mercado Pago
    "matt_tool", "matt_word", "matt_from",
    # Hotmart
    "ap", "hottok",
    # Commission Junction
    "pid", "sid",
    # ShareASale
    "afftrack", "merchantid",
    # Rakuten
    "ranmid", "raneaid",
    # Impact
    "irclickid",
    # Awin
    "awc",
    # Generic
    "aff_id", "affid", "affiliate", "ref_id", "partner_id", "utm_source",
}

_AFFILIATE_HOSTS = {
    "go.skimresources.com",
    "assoc-redirect.amazon.com",
    "mercadolivre.com.br/ssp",
}


def is_affiliate_url(url: str) -> bool:
    parsed = urlparse(url)
    if any(h in parsed.hostname or "" for h in _AFFILIATE_HOSTS):
        return True
    params = {k.lower() for k in parsed.query.split("&") if "=" in k}
    param_names = {p.split("=")[0] for p in params}
    return bool(param_names & _AFFILIATE_PARAMS)


def retailer_name(url: str) -> str:
    host = urlparse(url).hostname or ""
    for domain, name in _RETAILER_NAMES.items():
        if domain in host:
            return name
    clean = host.removeprefix("www.").split(".")[0]
    return clean.title()


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

def extract_meta(soup: BeautifulSoup) -> dict:
    title = description = image_url = brand = price = ""

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") not in ("Product", "ItemPage"):
                    continue
                title = title or item.get("name", "")
                description = description or item.get("description", "")
                img = item.get("image")
                if img and not image_url:
                    image_url = img[0] if isinstance(img, list) else img
                b = item.get("brand") or {}
                brand = brand or (b.get("name", "") if isinstance(b, dict) else str(b))
                offers = item.get("offers") or {}
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                price = price or str(offers.get("price", ""))
        except Exception:
            pass

    def og(prop):
        tag = soup.find("meta", property=f"og:{prop}") or soup.find(
            "meta", attrs={"name": f"og:{prop}"}
        )
        return (tag.get("content") or "").strip() if tag else ""

    title = title or og("title") or (soup.title.get_text(strip=True) if soup.title else "")
    description = description or og("description")
    image_url = image_url or og("image")

    if not image_url:
        tag = soup.find("meta", attrs={"name": "twitter:image"})
        if tag:
            image_url = (tag.get("content") or "").strip()

    return {
        "title": title,
        "description": description[:500] if description else "",
        "image_url": image_url,
        "brand": brand,
        "price": price,
    }


# ---------------------------------------------------------------------------
# MCP config reader
# ---------------------------------------------------------------------------

def _load_mcp_config() -> tuple[str, str]:
    root = Path(__file__).parent.parent
    mcp_json = root / ".mcp.json"
    if not mcp_json.exists():
        print("Error: .mcp.json not found. Copy from skills/mcp.json.example and fill in token.", file=sys.stderr)
        sys.exit(1)
    cfg = json.loads(mcp_json.read_text())
    server = cfg.get("mcpServers", {}).get("xcmagg-catalog", {})
    url = server.get("url", "").rstrip("/")
    token = server.get("headers", {}).get("Authorization", "")
    if not url or not token:
        print("Error: .mcp.json missing url or Authorization header.", file=sys.stderr)
        sys.exit(1)
    return url, token


# ---------------------------------------------------------------------------
# CLI subcommands
# ---------------------------------------------------------------------------

def cmd_scrape(url: str) -> None:
    soup = fetch_page(url)
    meta = extract_meta(soup)
    meta["retailer"] = retailer_name(url)
    print(json.dumps(meta, ensure_ascii=False))


def cmd_upload(shortcode: str, image_url: str) -> None:
    MAX_BYTES = 2 * 1024 * 1024  # 2MB — matches system upload limit

    mcp_url, token = _load_mcp_config()
    img_bytes, filename = fetch_image(image_url)

    if len(img_bytes) > MAX_BYTES:
        print(
            json.dumps({"error": f"image is {len(img_bytes) // 1024}KB, exceeds 2MB system limit"})
        )
        sys.exit(1)

    import requests as std_requests

    upload_url = f"{mcp_url}/upload"
    resp = std_requests.post(
        upload_url,
        data={"shortcode": shortcode, "is_primary": "true"},
        files={"image": (filename, img_bytes)},
        headers={"Authorization": token},
        timeout=30,
    )
    resp.raise_for_status()
    print(json.dumps(resp.json(), ensure_ascii=False))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    subcmd = sys.argv[1]
    try:
        if subcmd == "scrape":
            cmd_scrape(sys.argv[2])
        elif subcmd == "upload":
            if len(sys.argv) < 4:
                print("Usage: fetch.py upload <shortcode> <image-url>", file=sys.stderr)
                sys.exit(1)
            cmd_upload(sys.argv[2], sys.argv[3])
        else:
            print(f"Unknown subcommand: {subcmd!r}", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)
