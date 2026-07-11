---
title: "How did I start XCMAGG? A no-BS race-events aggregator"
description: "Why I pulled a dozen scattered Brazilian ticketing sites into one calendar — and why a JSONL file plus a static HTML page is the right way."
lang: en
publish_date: 2025-10-02
updated_date: 2025-10-02
translation_group: how-i-started-xcmagg
draft: false
tags: ["xcmagg", "web-scraping", "python", "duckdb", "indie-hacking"]
author: "Lucas Vido"
---

This is where the project actually started: I could not find MTB races!
By the way - I was in my cycling hyperfocus phase.

## Two itches

**One: the data is scattered.** If you ride in Brazil and want to know what races
are coming up, there is no single place to look. Every event lives on whichever
ticketing platform the organizer happened to pick — and there are about two dozen of
them. There is no shared calendar. The federations calendar does not link to the event-page.
Many organizers rely on Instagram page. You find out about a race when a friend posts it in a WhatsApp group three days before registration closes...

**Two: the existing sites have terrible filtering.** Even on the platforms that
do list events, the geography is broken. They filter by *their* idea of a region,
not by where you actually are. I live near Bragança Paulista, in São Paulo. Some
of the best riding near me is in **Sul de Minas** — close enough to drive to on a
Saturday, but on the other side of a state line. No tool lets me say "show me what's
within reasonable driving distance," because none of them know where the events
*are*, only which state dropdown they were filed under.

I decide to scrape them!

## What XCMAGG is

XCMAGG stands for **Cross Country Marathon AGGregator**. The job is unglamorous and
specific: visit every ticketing site that matters, pull out every cycling events, normalize the mess into one clean shape, and produce a single feed. So I know when and where each race happens.

## The "No-BS" thesis

The week before I started this project - A business partner once got an offer to acquire a
startup, and I was pulled into the due diligence. The thing was wildly
over-engineered for its stage: about **200 customers**, and roughly **R$17k/month**
in infrastructure. The cost scaled with each new customer so brutally that they had
*stopped taking on clients* — every new signup made the unit economics worse.
Insane. A company actively turning away revenue because its own architecture taxed
growth.

That's the anti-pattern XCMAGG is built against. The goal of this project is to hit
the target using the **least** amount of resources — not the most impressive stack.
A scraper writing a flat file, costs roughly the same whether it serves 200 people or 200,000. Cost efficiency isn't a vanity metric here; it's what lets a free tool stay free and a solo maker keep shipping.

Here's the architectural decision I'm proudest of, and it sounds like laziness
until you live with it:

> Serverless - 100% file-based.
> The entire product is a scraper that writes a **JSONL file**,
> plus a **static HTML page** that reads it.
> The "database" is JSON, CSV, and [DuckDB](https://duckdb.org/) files.

By serverless I mean truly serverless - not what AWS calls "cloud-hosted database".

No API. No SPA framework. No database-backed backend sitting in the request path.
The page you load is a flat HTML file that `fetch()`es one `data.jsonl` and
renders it in the browser.

This was an opinionated decision against slop. So many developers have pre-conceived cargo-cult pratices. This creates unecessary complexity - and bogs the companies down.

## Behind the build: Medallion Arch (Bronze → Silver → Gold)

An a previous consultancy on a startup - they had a API plugged into a Redis (which was the persistencey database). There was a nasty home-made ORM. Developers were unprepared do deal with a NoSQL. This was complex and caused many many bugs. The solution was cut the useless abstraction - call the Redis primitives - no wrapper, no abstraction - the API layer was the abstraction it self.
Using the wrong abstraction will cost a lot. 

Data scraping is complex - it can seem chaotic at times.
The crucial decision is where/when to apply abstraction.
> *ab-* — prefix meaning "away from"
> *traction* — to draw, pull
> scraper = pull away from... someone else's data?

The complexity lives in the pipeline that *produces* the file — not in serving it.
The scraper follows a medallion architecture. Data moves through three layers,
getting cleaner at each step.

### Bronze — raw scraping

> TL;DR -> How we download HTML/JSON/PDF to the filesystem.

Bronze is where the dirt comes in. Each source gets its own crawler — about a dozen
classes, one per website/api

Some sources are friendly JSON APIs. Some are HTML I parse with BeautifulSoup. Some
publish event details as **PDFs**, which I read with `pdfplumber`. A few sit behind
bot protection that rejects a normal `requests` call, so I use `curl_cffi` to
impersonate a real browser's TLS fingerprint.

And sometimes sources die. My crawler list has commented-out lines — sources that shut down,
or changed so much they weren't worth chasing anymore. Maintaining an aggregator
means accepting that your sources are a cat-and-mouse game.

### Silver — normalization

> TL;DR -> How to load data into DuckDB

Silver is where the chaos becomes structure. Two problems dominate: **dates** and
**locations**, and both arrive as free text written by humans.

Dates show up as ranges, single days, month names, ambiguous formats.
Locations are worse — "Serra da Mantiqueira," a venue name with no city, a city
spelled three different ways.

For the genuinely messy cases I lean on **LLM
agents** (OpenAI tool-calling) to parse free text into a structured
`{city, uf, ...}` object the rest of the pipeline can trust.

### Gold — publishing

> TL;DR -> How to enrich and publish results

Gold is gold. A DuckDB `COPY` query flattens everything into one
`data.jsonl`, and on the way out each event gets **geo-enriched**: I match its city
against an IBGE municipality database to attach a DDD (area code) plus latitude and
longitude.

That geo step is the answer to itch number two. Once every event has a real
lat/long, "what's near Bragança Paulista" — including races over in Sul de Minas —
becomes a distance calculation in the browser instead of a guess against a state
dropdown.

And every outbound link gets a `?utm_source=xcmagg` so I can see that the aggregator
actually sends organizers traffic.

One line of the finished `data.jsonl` looks like this:

```json
{"title":"Desafio Speed - Almenara 2026","url":"https://ticketing.example/e/desafio-speed-almenara-2026?utm_source=xcmagg","start_date":"22-08-2026","city":"Almenara","uf":"MG","ddd":"33","latitude":-16.1785,"longitude":-40.6942,"sport":"Corrida de Rua"}
```

That single line — clean, located, attributed — is the entire output of all that
poor man's Databricks. Multiply it by a few hundred and you have the calendar.

## Solodev fights against complexity/slop

Every architectural choice is really a choice about how much operational weight
I'm willing to carry. I build this alone. Just like a bike: the lighter, the better.

- I can trigger this pipeline from my local PC - No cost
- LLM costs are kept to a minimum - LLM is called only on edge cases
- Server costs are near zero.
- Deploy is like its the 2000's

Check the end result of [event calendar](https://racefeed.com.br/events/)

## What's next

The aggregator code-base keeps growing — more sources, smarter proximity filtering, and
eventually a public calendar anyone can open to find their next race. The hard part
is done: there's one clean file that knows where and when races happen.
