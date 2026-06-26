# TODO / Known Issues / Planned Features

## Monetization
- Affiliate links in Fuel Plan — carb gels, nutrition products (already partially done, needs expansion)
- Event registration partnerships — need more traffic first to justify

## Known Issues
- Gear Matrix is crap — needs full rework

## Security
- `data.jsonl` is a public single-file dump of all events — trivial to scrape. Fix: kill it when Django SSR is ready (paginated API replaces it).
- Add `robots.txt` rules blocking LLM crawlers (GPTBot, ClaudeBot, etc.)
- Consider Cloudflare free tier for rate limiting / bot protection

## Sport Classification
- Root cause: `RawEvent` has no `description` field — classifier only sees `title + local`
- Fix: add `description: str = ''` to `RawEvent` + `Extractor` base, pass to `classify_sport()`
- Crawlers to update first (have detail pages): TicketBr, TicketSports, ActiveSports, Peloto, Atletis
- PDFs / federation calendars: skip (no descriptions available)

## SEO
- Plan B: city landing pages → `plans/plan-b-seo-city-pages.md`
- OG cards for location calendar — make shared link visually clickable (rich preview image, compelling title/description)
