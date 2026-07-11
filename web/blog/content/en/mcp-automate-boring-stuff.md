---
title: "MCP — Automate the Boring Stuff"
description: "Catalog curation is the unglamorous core of a product site. An MCP server and a slash command cut it from ten minutes of manual work to two."
lang: en
publish_date: 2026-07-09
updated_date: 2026-07-09
translation_group: mcp-automate-boring-stuff
draft: true
tags: ["mcp", "django", "python", "affiliate", "indie-hacking", "claude"]
author: "Lucas Vido"
---

A product site lives and dies by its catalog. Not the code, not the design — the data. Get behind on curation and you've built a beautiful, empty shelf.

For a long time, adding a product to this site cost me ten minutes. Open the product page, copy the title, strip the brand name out of it, upload the photo, pick the category, paste the affiliate URL, remember to add `rel="sponsored"` so Google doesn't penalize me for an undisclosed link. Twenty products burns half your afternoon on data entry.

That ten-minute task is now two minutes. The change was a Model Context Protocol server wired directly to my Django database.

## MCP is plumbing for repetitive tasks

MCP is an open protocol for exposing app functionality to an AI assistant as callable tools. You define a Python function, decorate it with `@mcp.tool()`, run it as an HTTP server, and Claude can call it mid-conversation as naturally as using a search engine.

The mental model is simple: *boring, repetitive tasks with a clear structure* are where MCP earns its keep. Not generation, not reasoning — execution. "Look up whether this manufacturer already exists. If not, create it. Then create the item. Then upload the photo." That's a flowchart. Flowcharts should run themselves.

```python
@mcp.tool()
async def create_catalog_item(
    title: str, manufacturer: str, category: str,
    description: str = "", links: list[dict] = [],
) -> dict:
    """Create or update a catalog item with affiliate links."""
    ...
    return {"url": item.get_absolute_url(), "shortcode": node.shortcode}
```

No API layer to maintain, no REST endpoint, no serializer. The MCP server *is* the interface — authenticated by a bearer token, invisible on the public internet, calling the ORM directly.

## The bottleneck was never the code

Every step in the workflow is individually trivial: a form POST here, a file upload there. The cost is the context-switching — find the page, find the image, open the admin, navigate the dropdowns, verify the affiliate link. Each step is five seconds; the interruptions between them are what burns the time.

A slash command collapses all of that into one conversation:

```
/add-product https://...
```

Claude scrapes the product page locally, shows me the title to confirm, resolves the manufacturer and category against the database, creates the item, and uploads the photo directly to the server — image bytes never pass through Claude, they go straight from my machine to the server. My only decision is whether the title looks right.

The ten minutes was never ten minutes of thinking. It was ten minutes of mechanical steps I'd done identically a hundred times before — now encoded in a tool.

## Data curation is the product

The unglamorous truth about a useful site is that content is the work. The catalog, the descriptions, the photos, the affiliate links — that's what a visitor sees and comes back for. The code is infrastructure.

Before: I'd add products in batches, reluctantly, when the backlog got embarrassing. After: one product takes as long as opening a browser tab. That's not a productivity gain — it's a different relationship with the task. MCP didn't change what the site does. It changed whether I'm willing to keep doing it.
