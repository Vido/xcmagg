# TODO / Known Issues / Planned Features

## Completed — 2026-06-29 (Performance + SEO session)
- Preconnect/dns-prefetch for Tailwind, Alpine, HTMX CDNs + preload hint for Tailwind script
- ARIA: mobile menu (aria-expanded, aria-controls, aria-label), all dropdowns, flash close button, action buttons
- CLS: x-cloak on mobile menu + all dropdowns, logo width/height attributes
- Flowbite extracted from base.html → per-template blocks; modal added to item_editor (replaces browser confirm)
- robots.txt: allow-by-default, blocks only /accounts/ /magic/ /go2/ /lookups/ /vote/ /rate/ /comments/new/ /photos/upload/ /post/new/ /catalog/new/ /inventory/new/
- Image sitemap: custom sitemap.xml template (xmlns:image), PostSitemap added, CatalogItemSitemap + PostSitemap emit image entries
- fetchpriority="high" on gallery primary + first card in list loops; loading="lazy" on remaining cards
- Date/meta text contrast: gray-400 → gray-600 (light) / gray-500 → gray-400 (dark) across post cards, blog list, blog sidebar, article meta

## Performance / Build (next sprint)
- **Tailwind build step** — compile + purge CSS, drop CDN entirely → `plans/tailwind-compile-purge-css-for-prod.md`
- **PWA** — manifest (installable), service worker CDN cache (kills 810ms Tailwind delay after first visit), offline tools → `plans/pwa-mobile-ux.md`

## Email / Magic Links (HIGH PRIORITY — blocks user acquisition)
- Verify sender `no-reply@racefeed.com.br` in Postmark (DKIM + Return-Path DNS on `racefeed.com.br`)
- Confirm `POSTMARK_SERVER_TOKEN` is set in `web/.env` on server (file exists but token must be valid)
- Deploy + test full flow: register → email received → magic link works

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
