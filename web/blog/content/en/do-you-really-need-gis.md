---
title: "Do you really need GIS?"
description: "Most sites filter locations as nested categories. Cities are 2D. A free IBGE dataset and one formula from high school — no GIS needed."
lang: en
publish_date: 2026-07-03
updated_date: 2026-07-03
translation_group: do-you-really-need-gis
draft: true
use_katex: true
tags: ["racefeed", "geo", "sqlite", "python", "indie-hacking", "yagni"]
author: "Lucas Vido"
---

There's a classic engineering approximation: **π ≈ 3**.
Not flying a plane. Not sending a rocket. Just doing structural estimates.
Close enough — and crucially — *fast*.

The same logic applies to geography in most web applications.

## Cities as categories — a bureaucrat's approach

Go to nearly any event ticketing site in Brazil and look at their location filter.
You get a dropdown: **Country → State → City**.
<a href="https://www.ticketsports.com.br/calendar/filters?city=S%C3%A3o+Paulo&state=SP&country=BR" rel="nofollow">https://www.ticketsports.com.br/calendar/filters?city=São+Paulo&state=SP&country=BR</a> is a good example.

This model treats geography as a taxonomy — a bureaucratic hierarchy.
And it fails the moment reality doesn't respect those borders.

I live near Bragança Paulista, São Paulo state.
Some of the best MTB races near me are in **Sul de Minas** — close enough to drive Saturday morning, but across a state line.
No dropdown will ever show me those races when I filter "São Paulo".

It fails the other way too. <a href="https://en.wikipedia.org/wiki/Presidente_Prudente" rel="nofollow">Presidente Prudente</a> and <a href="https://en.wikipedia.org/wiki/S%C3%A3o_Jos%C3%A9_do_Rio_Preto" rel="nofollow">São José do Rio Preto</a> are both São Paulo state — same drawer. But they're **+400 km away**. A filter that shows me those races alongside the ones I can actually drive to is useless noise.

The filter doesn't know where I *am* — it only knows which drawer the organizer filed the race into.

> Cities are not categories. They are **points on a sphere**.

## The epiphany

XCMAGG already pulls city and state from every event it scrapes.
At the Gold layer, each event gets geo-enriched by joining against the **IBGE municipalities database** — a free, complete JSON file with every Brazilian city: name, UF, DDD, latitude, longitude.

```python
# gold.py — the join that turns text into coordinates
LEFT JOIN geo g
    ON strip_accents(LOWER(TRIM(e.location->>'city'))) = strip_accents(LOWER(TRIM(g.nome)))
    AND UPPER(TRIM(e.location->>'uf')) = UPPER(TRIM(g.uf))
```

The output `data.jsonl` already had `city` and `uf`.
After this join, it also has `latitude` and `longitude`.

That's it. One join. No GIS extension. No spatial index. No PostGIS.
The database I already needed for DDD lookup gave me 2D coordinates for free.

## Three levels of "close enough"

Once you have lat/lon on every event, "find what's near me" is a math problem.
Three options, in increasing precision:

**1. Bounding box** — very coarse, almost free:

$$|\phi_2 - \phi_1| < \delta \quad \text{and} \quad |\lambda_2 - \lambda_1| < \delta$$

Works. Terrible near the poles (not our problem in Brazil). Good enough for a prototype.

**2. Pythagorean distance** — what teenagers learn:

$$d = \sqrt{(\phi_2-\phi_1)^2 + (\lambda_2-\lambda_1)^2}$$

Treats Earth as flat. Degrades over large distances.
For "races within 150 km of my city" in Southeast Brazil — perfectly fine.

**3. Haversine** — the full spherical model:

$$d = 2R \arcsin\!\left(\sqrt{\sin^2\!\frac{\Delta\phi}{2} + \cos\phi_1\cos\phi_2\sin^2\!\frac{\Delta\lambda}{2}}\right)$$

True great-circle distance on a sphere. Still just $\arcsin$ and $\sqrt{\phantom{x}}$ — bread-and-butter trig.
And SQLite handles it natively, no extensions:

```sql
CAST(ROUND(6371 * 2 * asin(sqrt(
    power(sin((radians(g.latitude)  - radians(?)) / 2), 2) +
    cos(radians(?)) * cos(radians(g.latitude)) *
    power(sin((radians(g.longitude) - radians(?)) / 2), 2)
))) AS INTEGER) AS dist_km
```

Same formula, two runtimes: SQLite on the server for the Django API, browser JS for the static calendar.
No GIS anywhere.

I went with Haversine. Not because it was necessary — for Brazil, Pythagorean would survive —
but because it cost nothing extra and it's the right model.
$\pi = 3.14159$, not 3. We can afford the extra digits here.

## Frictionless location

The natural next move is to ask the user where they are.
"Allow Location" — the browser prompt. Invasive. Slow. Half the users dismiss it on reflex.

Better: **GeoIP**. Match the user's IP against a city-level database.
Not precise to the street — but we don't need the street.
We need to know they're probably in Campinas, not Porto Alegre.
That's enough to pre-filter the calendar and surface the relevant events before they've clicked anything.

Zero friction. No permission prompt. And it opens the door to more targeted affiliate links — if I know you're riding in Minas, I can surface deals on Minas-local events.

## When would you actually need GIS?

A true GIS stack — PostGIS, QGIS, R-Trees, ellipsoidal projections — is a powerful gun.
There are legitimate targets for it:

- Polygon queries: "is this point inside this administrative boundary?"
- Routing: shortest path on a road network
- Raster analysis: elevation models, land cover
- Millions of spatial queries per second with sub-millisecond latency

For "show me MTB races within 200 km" — it's a gun to kill a mosquito.
And beyond the overkill, there's a skill problem: most devs don't know GIS.
It's a separate discipline with its own tooling, projections, and failure modes.
Reaching for it as a default adds operational weight and a learning curve for zero benefit.

The lazy crescent wins: **IBGE JSON + Haversine + SQLite**.
Free. Stateless. Ships in a lunch break.

## What it unlocked

The static calendar at [racefeed.com.br/events](https://racefeed.com.br/events/) now has:

- **Near me** — one click, filters to events within a configurable radius
- **Sort by distance** — closest race first, not just soonest
- **~X km** displayed on each card when near-me is active

None of this required a spatial database.
It required treating cities as what they already are: two numbers on a coordinate system.

The IBGE database gave me those numbers for free, on a JOIN I already needed for DDD codes.
The math was on my high school transcript.

Sometimes the hardest part of building is resisting the urge to reach for the impressive tool.
