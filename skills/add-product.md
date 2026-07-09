Add a product to the racefeed catalog from a marketplace URL.

The URL to add is: $ARGUMENTS

## Workflow

**Step 1 — Scrape product page (local machine, residential IP)**

```bash
cd xcmagg && uv run python fetch.py scrape "$ARGUMENTS"
```

Show the user the extracted data: title, brand, price, image_url, retailer.

**Title cleanup**: if the brand name appears at the start of the title, strip it.
Example: brand="Vittoria", title="Vittoria Peyote XC Trail…" → use "Peyote XC Trail…"

Ask the user to confirm or correct the final title before proceeding.

**Step 2 — Resolve manufacturer**

Call `search_brands` MCP tool with the brand name from the scraped data.

- Found → use the returned slug
- Not found → ask the user to confirm creation, then call `create_manufacturer`

**Step 3 — Resolve category**

Call `search_categories` MCP tool with the product type inferred from the title/description.

- Found → use the returned slug
- Not found → call `search_categories("")` to list all, ask the user to pick one

**Step 4 — Create catalog item**

Call `create_catalog_item` MCP tool:
- `title`: confirmed product title
- `manufacturer`: resolved slug
- `category`: resolved slug
- `description`: scraped description (already trimmed to 500 chars)
- `links`: `[{"text": "Comprar no {retailer}", "url": "$ARGUMENTS", "is_affiliate": false}]`
  where `{retailer}` is the `retailer` field from Step 1 (e.g. "Comprar no Mercado Livre").
  `is_affiliate` defaults to `false` — only set `true` if the user confirms we have
  an active affiliate program with that retailer for this link.

This returns `{"url": "...", "shortcode": "..."}`.

**Step 5 — Upload photo**

If `image_url` was found in Step 1, upload it directly to the server (no bytes through Claude):

```bash
cd xcmagg && uv run python fetch.py upload <shortcode> <image_url>
```

This is the only upload path — the MCP server has no server-side image-fetch tool
(removed: fetched from a datacenter IP, and retailers like AliExpress returned 403
for those; the local machine's residential IP works fine).

**Step 6 — Report**

Show the user the catalog item URL from Step 4.
